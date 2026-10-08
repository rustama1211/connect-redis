import {SessionData, Store} from "express-session"
import knexConstructor, {Knex} from "knex"
import {
  dateAsISO,
  expiredCondition,
  getMssqlFastQuery,
  getMysqlFastQuery,
  getPostgresFastQuery,
  getSqliteFastQuery,
  isDbSupportJSON,
  isMSSQL,
  isMySQL,
  isOracle,
  isPostgres,
  isSqlite3,
  timestampTypeName,
} from "./utils"

// Store callbacks are invoked exactly once and outside of try blocks: an
// error thrown by the caller's callback must not be caught here and reported
// back to that same callback as a store error.
const noop = (_err?: unknown, _data?: any) => {}

interface NormalizedRedisClient {
  get(key: string): Promise<string | null>
  set(key: string, value: string, ttl?: number): Promise<string | null>
  expire(key: string, ttl: number): Promise<number | boolean>
  scanIterator(match: string, count: number): AsyncIterable<string>
  del(key: string[]): Promise<number>
  mget(key: string[]): Promise<(string | null)[]>
}

interface Serializer {
  parse(s: string): SessionData | Promise<SessionData>
  stringify(s: SessionData): string
}

interface AdditionalSessionField {
  name: string
  type: string
  isSaveValue?: boolean
  // mapping field if exists
  mappingField?: Record<string, string>
}
interface RedisStoreOptions {
  client: any
  prefix?: string
  clientPrefix?: string
  scanCount?: number
  serializer?: Serializer
  ttl?: number | {(sess: SessionData): number}
  disableTTL?: boolean
  disableTouch?: boolean
  cleanupInterval: number // 0 disables
  createTable: boolean
  knex: Knex
  onDbCleanupError: (err: unknown) => void
  tableName: string
  sidFieldName: string
  additionalSesionFields?: Array<AdditionalSessionField>
}

let hasTableSession = false

export class RedisStore extends Store {
  client: NormalizedRedisClient
  prefix: string
  clientPrefix: string
  scanCount: number
  serializer: Serializer
  ttl: number | {(sess: SessionData): number}
  disableTTL: boolean
  disableTouch: boolean
  options: RedisStoreOptions
  nextDbCleanup: NodeJS.Timeout | undefined
  ready: Promise<unknown>

  constructor(incomingOptions: Partial<RedisStoreOptions>) {
    super()
    this.prefix =
      incomingOptions.prefix == null ? "sess:" : incomingOptions.prefix
    this.clientPrefix =
      incomingOptions.clientPrefix == null ? "" : incomingOptions.clientPrefix
    this.scanCount = incomingOptions.scanCount || 100
    this.serializer = incomingOptions.serializer || JSON
    this.ttl = incomingOptions.ttl || 86400 // One day in seconds.
    this.disableTTL = incomingOptions.disableTTL || false
    this.disableTouch = incomingOptions.disableTouch || false
    this.client = this.normalizeClient(incomingOptions.client)

    const options = (this.options = {
      cleanupInterval: 60000,
      createTable: true,
      sidFieldName: "sid",
      tableName: "sessions",
      onDbCleanupError: (err: unknown) => {
        console.error(err)
      },
      ...incomingOptions,
      knex:
        incomingOptions.knex ??
        knexConstructor({
          client: "sqlite3",
          connection: {
            filename: "connect-session-knex.sqlite",
          },
        }),
    } as RedisStoreOptions)

    const {cleanupInterval, createTable, knex, sidFieldName, tableName} =
      options

    this.ready = (async () => {
      if (!hasTableSession) {
        hasTableSession = await knex.schema.hasTable(tableName)
      }
      if (!hasTableSession) {
        if (!createTable) {
          throw new Error(`Missing ${tableName} table`)
        }
        const supportsJson = await isDbSupportJSON(knex)

        await knex.schema.createTable(tableName, (table) => {
          table.string(sidFieldName).primary()
          if (supportsJson) {
            table.json("sess").notNullable()
          } else {
            table.text("sess").notNullable()
          }
          if (isMySQL(knex) || isMSSQL(knex)) {
            table.dateTime("expired").notNullable().index()
          } else {
            table.timestamp("expired").notNullable().index()
          }
          if (options.additionalSesionFields) {
            options.additionalSesionFields.forEach(
              (field: AdditionalSessionField) => {
                ;(table as any)[field.type](field.name).nullable()
              },
            )
          }
        })
        hasTableSession = true
      }

      if (cleanupInterval > 0) {
        this.dbCleanup()
      }
    })()
  }

  // Create a redis and ioredis compatible client
  private normalizeClient(client: any): NormalizedRedisClient {
    let isRedis = "scanIterator" in client
    return {
      get: (key) => client.get(key),
      set: (key, val, ttl) => {
        if (ttl) {
          return isRedis
            ? client.set(key, val, {EX: ttl})
            : client.set(key, val, "EX", ttl)
        }
        return client.set(key, val)
      },
      del: (key) => client.del(key),
      expire: (key, ttl) => client.expire(key, ttl),
      mget: (keys) => (isRedis ? client.mGet(keys) : client.mget(keys)),
      scanIterator: (match, count) => {
        if (isRedis) return client.scanIterator({MATCH: match, COUNT: count})

        // ioredis impl.
        return (async function* () {
          let [c, xs] = await client.scan("0", "MATCH", match, "COUNT", count)
          for (let key of xs) yield key
          while (c !== "0") {
            ;[c, xs] = await client.scan(c, "MATCH", match, "COUNT", count)
            for (let key of xs) yield key
          }
        })()
      },
    }
  }

  async get(sid: string, cb = noop) {
    let key = this.prefix + sid
    let sess
    try {
      let data = await this.client.get(key)
      //data = (await this.db_get(key, cb)) as string | null
      if (data) sess = await this.serializer.parse(data)
    } catch (err) {
      return cb(err)
    }
    if (!sess) return cb()
    return cb(null, sess)
  }

  async set(sid: string, sess: SessionData, cb = noop) {
    let key = this.prefix + sid
    let ttl = this._getTTL(sess)
    // an already expired session is removed instead of saved
    if (ttl <= 0) return this.destroy(sid, cb)
    try {
      let val = this.serializer.stringify(sess)
      if (this.disableTTL) {
        await this.client.set(key, val)
        await this.db_set(sid, sess)
      } else {
        await this.db_set(sid, sess, ttl)
        await this.client.set(key, val, ttl)
      }
    } catch (err) {
      return cb(err)
    }
    return cb()
  }

  async db_get(
    sid: string,
    callback: (err: any, session?: SessionData | null) => void,
  ) {
    let session: SessionData | null = null
    try {
      await this.ready
      const {knex, tableName, sidFieldName} = this.options
      const condition = expiredCondition(knex)

      const response = await knex
        .select("sess")
        .from(tableName)
        .where(sidFieldName, "=", sid)
        .andWhereRaw(condition, dateAsISO(knex))

      if (response[0]) {
        session = response[0].sess
        if (typeof session === "string") {
          session = JSON.parse(session)
        }
      }
    } catch (err) {
      callback?.(err)
      throw err
    }
    callback?.(null, session)
    return session
  }

  async db_set(
    sid: string,
    session: SessionData,
    ttl?: number,
    callback?: (err?: any) => void,
  ) {
    try {
      await this.ready
      const {knex, tableName, sidFieldName} = this.options
      let {maxAge} = session.cookie
      maxAge = ttl || maxAge
      const now = new Date().getTime()
      const expired = maxAge ? now + maxAge * 1000 : now + 86400000 // 86400000 = add one day
      const sess = JSON.stringify(session)

      const dbDate = dateAsISO(knex, expired)

      if (isSqlite3(knex)) {
        // sqlite optimized query
        await knex.raw(getSqliteFastQuery(tableName, sidFieldName), [
          sid,
          dbDate,
          sess,
        ])
      } else if (isPostgres(knex) && parseFloat(knex.client.version) >= 9.2) {
        // only handling newer postgresql
        let extrafield: Array<Array<string>> = []
        let extravalue: Array<string> = []
        if (this.options.additionalSesionFields) {
          this.options.additionalSesionFields.forEach((obj) => {
            if ((session as any)[obj.name] && obj.isSaveValue) {
              if (extrafield.length == 0) {
                extrafield.push([obj.name])
                extrafield.push([
                  ["uuid", "json"].includes(obj.type) ? `?::${obj.type}` : "?",
                ])
                extravalue.push(
                  obj.type == "json"
                    ? JSON.stringify((session as any)[obj.name])
                    : (session as any)[obj.name],
                )
              } else if (extrafield.length == 2) {
                extrafield[0].push(obj.name)
                extrafield[1].push(
                  ["uuid", "json"].includes(obj.type) ? `?::${obj.type}` : "?",
                )
                extravalue.push(
                  obj.type == "json"
                    ? JSON.stringify((session as any)[obj.name])
                    : (session as any)[obj.name],
                )
              }
            }
          })
        }
        let defaultValue = [sid, dbDate, sess]
        if (extravalue.length > 0) {
          extravalue.forEach((value) => {
            defaultValue.push(value)
          })
        }
        // postgresql optimized query
        // add transaction for race condition fix
        try {
          await knex.transaction(async (trx) => {
            await knex
              .raw(
                getPostgresFastQuery(tableName, sidFieldName, extrafield),
                defaultValue,
              )
              .transacting(trx)
          })
        } catch (error) {
          // error handler
          console.error(error)
        }
      } else if (isMySQL(knex)) {
        await knex.raw(getMysqlFastQuery(tableName, sidFieldName), [
          sid,
          dbDate,
          sess,
        ])
      } else if (isMSSQL(knex)) {
        await knex.raw(getMssqlFastQuery(tableName, sidFieldName), [
          sid,
          dbDate,
          sess,
        ])
      } else {
        await knex.transaction(async (trx) => {
          const foundKeys = await trx
            .select("*")
            .forUpdate()
            .from(tableName)
            .where(sidFieldName, "=", sid)

          if (foundKeys.length === 0) {
            await trx.from(tableName).insert({
              [sidFieldName]: sid,
              expired: dbDate,
              sess,
            })
          } else {
            await trx(tableName).where(sidFieldName, "=", sid).update({
              expired: dbDate,
              sess,
            })
          }
        })
      }
    } catch (err) {
      callback?.(err)
      throw err
    }
    callback?.()
  }

  async touch(sid: string, sess: SessionData, cb = noop) {
    let key = this.prefix + sid
    if (this.disableTouch || this.disableTTL) return cb()
    try {
      await this.client.expire(key, this._getTTL(sess))
      await this.db_touch(sid, sess)
    } catch (err) {
      return cb(err)
    }
    return cb()
  }

  async db_touch(sid: string, session: SessionData, callback?: () => void) {
    await this.ready
    const {knex, tableName, sidFieldName} = this.options

    if (session && session.cookie && session.cookie.expires) {
      const condition = expiredCondition(knex)

      await knex(tableName)
        .where(sidFieldName, "=", sid)
        .andWhereRaw(condition, dateAsISO(knex))
        .update({
          expired: dateAsISO(knex, session.cookie.expires),
        })
    }
    callback?.()
  }

  async destroy(sid: string, cb = noop) {
    let key = this.prefix + sid
    try {
      await this.client.del([key])
      await this.db_destroy(sid)
    } catch (err) {
      return cb(err)
    }
    return cb()
  }

  async clear(cb = noop) {
    try {
      let keys = await this._getAllKeys()
      if (keys.length) {
        await this.client.del(keys)
        await this.db_clear()
      }
    } catch (err) {
      return cb(err)
    }
    return cb()
  }

  async length(cb = noop) {
    let keys
    try {
      keys = await this._getAllKeys()
    } catch (err) {
      return cb(err)
    }
    return cb(null, keys.length)
  }

  async ids(cb = noop) {
    let len = (this.clientPrefix + this.prefix).length
    let keys
    try {
      keys = await this._getAllKeys()
    } catch (err) {
      return cb(err)
    }
    return cb(
      null,
      keys.map((k) => k.substring(len)),
    )
  }

  async all(cb = noop) {
    let len = (this.clientPrefix + this.prefix).length
    let results: SessionData[] = []
    try {
      let keys = await this._getAllKeys()
      if (keys.length) {
        let data = await this.client.mget(keys)
        results = data.reduce((acc, raw, idx) => {
          if (!raw) return acc
          let sess = this.serializer.parse(raw) as any
          sess.id = keys[idx].substring(len)
          acc.push(sess)
          return acc
        }, results)
      }
    } catch (err) {
      return cb(err)
    }
    return cb(null, results)
  }

  async db_destroy(sid: string, callback?: (err?: any) => void) {
    let retVal
    try {
      await this.ready
      const {knex, tableName, sidFieldName} = this.options

      retVal = await knex.del().from(tableName).where(sidFieldName, "=", sid)
    } catch (err) {
      callback?.(err)
      throw err
    }
    callback?.()
    return retVal
  }

  async db_length(callback: (err: any, length?: number) => void) {
    let length
    try {
      await this.ready
      const {knex, tableName, sidFieldName} = this.options

      const response = await knex
        .count(`${sidFieldName} as count`)
        .from(tableName)

      if (response.length === 1 && "count" in response[0]) {
        length = +(response[0].count ?? 0)
      }
    } catch (err) {
      callback?.(err)
      throw err
    }
    callback?.(null, length)
    return length
  }

  async db_clear(callback?: (err?: any) => void) {
    let res
    try {
      await this.ready
      const {knex, tableName} = this.options

      res = await knex.del().from(tableName)
    } catch (err) {
      callback?.(err)
      throw err
    }
    callback?.()
    return res
  }

  async db_all(
    callback: (
      err: any,
      obj?: SessionData[] | {[sid: string]: SessionData} | null,
    ) => void,
  ) {
    let sessions
    try {
      await this.ready
      const {knex, tableName} = this.options

      const condition = expiredCondition(knex)
      const rows = await knex
        .select("sess")
        .from(tableName)
        .whereRaw(condition, dateAsISO(knex))

      sessions = rows.map((row) => {
        if (typeof row.sess === "string") {
          return JSON.parse(row.sess)
        }

        return row.sess
      })
    } catch (err) {
      callback?.(err)
      throw err
    }
    callback?.(undefined, sessions)
    return sessions
  }

  private async dbCleanup() {
    const {cleanupInterval, knex, tableName, onDbCleanupError} = this.options

    try {
      await this.ready

      let condition = `expired < CAST(? as ${timestampTypeName(knex)})`
      if (isSqlite3(knex)) {
        condition = "datetime(expired) < datetime(?)"
      } else if (isOracle(knex)) {
        condition = `"expired" < CAST(? as ${timestampTypeName(knex)})`
      }
      await knex(tableName).del().whereRaw(condition, dateAsISO(knex))
    } catch (err: unknown) {
      onDbCleanupError?.(err)
    } finally {
      if (cleanupInterval > 0) {
        this.nextDbCleanup = setTimeout(() => {
          this.dbCleanup()
        }, cleanupInterval).unref()
      }
    }
  }

  private _getTTL(sess: SessionData) {
    if (typeof this.ttl === "function") {
      return this.ttl(sess)
    }

    let ttl
    if (sess && sess.cookie && sess.cookie.expires) {
      let ms = Number(new Date(sess.cookie.expires)) - Date.now()
      ttl = Math.ceil(ms / 1000)
    } else {
      ttl = this.ttl
    }
    return ttl
  }

  private async _getAllKeys() {
    let pattern = this.clientPrefix + this.prefix + "*"
    let keys = []
    for await (let key of this.client.scanIterator(pattern, this.scanCount)) {
      if (this.clientPrefix.length) {
        const regPattern = new RegExp("^" + this.clientPrefix)
        key = key.replace(regPattern, "")
      }
      keys.push(key)
    }
    return keys
  }
}

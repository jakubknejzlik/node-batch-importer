genericPool = require('generic-pool')
mysql = require('mysql')

class MySQLConnection extends Object
  constructor:(@name, @options)->
    super
#    @pool = mysql.createPool(@options)
    @pool = genericPool.Pool(
      name: @name
      max:1
      create: (callback) =>
        conn = mysql.createConnection(@options)
        callback null, conn
      destroy: (connection) =>
        connection.destroy()
    )
  query:(sql, rowCallback, callback, batchSize) ->
    @pool.acquire (err,connection)=>
      return callback(err) if err
      data = []
      columns = []
      rows = 0
      error = null
      query = connection.query(sql)
      query.on("error", (err) =>
        error = err
      ).on("fields", (fields) ->
        for field in fields
          columns.push(field.name)
      ).on("result", (row) ->
        rows++
        _row = []
        rowCallback row if rowCallback
        for column in columns
          _row.push row[column]
        data.push _row
        if batchSize and data.length >= batchSize
          callback null, data, columns, false
          data = []
      ).on "end", =>
        callback error, data, columns, true
        @pool.release(connection);

  createSQL:(columns, rows, table) ->
    keys = []
    for i of columns
      column = columns[i]
#      if (column isnt @options.mergeKey) or columns.length <= 2
      keys.push column + " = VALUES(" + column + ")"
#    keys.push @options.mergeKey+" = VALUES("+@options.mergeKey+")"  if keys.length is 0
    sql = "INSERT INTO `" + table + "` (" + columns.join(",") + ") VALUES "
    for i of rows
      for ii of rows[i]
        if rows[i][ii] and rows[i][ii].substring and rows[i][ii].substring(0, 2) is "`("
          rows[i][ii] = rows[i][ii].replace(/^`\(/g, "(").replace(/\)`$/g, ")")
        else #.replace(/^'\(/g,'(').replace(/\)'$/g,')').replace(/\\'/g,"'");
        rows[i][ii] = mysql.escape(rows[i][ii])
      rows[i] = rows[i].join(",")
    sql += "(" + rows.join("),(") + ")"
    sql += " ON DUPLICATE KEY UPDATE " + keys.join(",")
    sql

  close:->
    @pool.drain =>
      @pool.destroyAllNow()
      return

module.exports = MySQLConnection
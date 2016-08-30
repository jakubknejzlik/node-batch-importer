Connection = require('tedious').Connection
Request = require('tedious').Request
genericPool = require('generic-pool')

url = require('url')

class MSSQLConnection extends Object
  constructor:(@name,@options)->
    super
    @pool = genericPool.Pool({
        name: @name,
        max: 1,
        create:(callback)=>
          cnfg = @_parseOptions(@options)
          cnfg.options = cnfg.options or {}
          cnfg.options.requestTimeout = cnfg.options.requestTimeout or 60*5*1000
          connection = new Connection(cnfg);
          connected = false;
          connection.on 'connect',(err)=>
            if connected
              return;
            connected = true;
            callback(err,connection);
        destroy:(connection)=>
          connection.close();
      })

  query:(sql,rowCallback,callback,batchSize)->
    @pool.acquire (err,connection)=>
      data = []
      columns = []
      rows = 0

      request = new Request sql,(err,rowCount)=>
#        console.log('row count',err,rowCount)
        callback(err,data,columns,true);
        @pool.release(connection);

      request
        .on 'err',(err)=>
          callback(err);
          @pool.release(connection);
        .on 'columnMetadata',(metadata)=>
#          console.log(metadata)
          for meta in metadata
            columns.push(meta.colName);
          columns = columns.filter (e, i, arr)->
            return arr.lastIndexOf(e) is i
        .on 'row', (row)=>
          _row = []
          __row = {}
          for col in row
            __row[col.metadata.colName] = col.value;
          if rowCallback
            rowCallback(__row);
          for column in columns
            _row.push(__row[column]);

          data.push(_row);
          if batchSize && data.length >= batchSize
            callback(null,data,columns,false)
            data = [];
          rows++;
        .on 'done',(rowCount,more)=>
#			    	console.log('done');

      connection.execSqlBatch(request);

  createSQL:(columns, rows, table) ->
    keys = []
    _rows = []
    for i of columns
      keys.push columns[i] + " = VALUES(" + columns[i] + ")"
    for i of rows
      _rows[i] = _rows[i] or []
      for ii of rows[i]
        _rows[i][ii] = mysql.escape(rows[i][ii])
        if _rows[i][ii]?.replace
          _rows[i][ii] = _rows[i][ii].replace(/\\'/g,"''")
        _rows[i][ii] = "null"  if _rows[i][ii] is null
      _rows[i] = _rows[i].join(",")
    sql = "MERGE `" + table + "` USING (VALUES(" + _rows.join("),(") + ")) AS foo(" + columns.join(",") + ") ON `" + table + "`.id = foo.id"
    columnAssigns = []
    for i of columns
      columnAssigns.push columns[i] + "=foo." + columns[i]
    sql += " WHEN MATCHED THEN UPDATE SET " + columnAssigns.join(",")
    sql += " WHEN NOT MATCHED THEN INSERT (" + columns.join(",") + ") VALUES(" + columns.join(",") + ");"
    sql

  close:->
    @pool.drain =>
      @pool.destroyAllNow()
      return

  _parseOptions:(options)->
    if typeof options isnt 'string'
      return options
    parsedUrl = url.parse(options)
    splittedAuth = parsedUrl.auth.split(':')

    userName: splittedAuth[0]
    password: splittedAuth[1]
    server: parsedUrl.host
    database: parsedUrl.pathname



module.exports = MSSQLConnection
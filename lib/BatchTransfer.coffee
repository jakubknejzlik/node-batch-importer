async = require('async')


SQL_RERTY_COUNT = 5


class BatchImporterTransfer extends Object
  constructor:(@sourceConnection, @destinationConnection, @destinationTable, @sql, @rowCallback, @callback)->
    super
    @logProgress = no
    @err = null
    @rows = 0
    @queueItemsCount = 0
    @queue = async.queue((sql, callback)=>
      @printProgress()
      @destinationConnection.query(sql.query, null, (err,res)=>
#        console.log('..',@sql,res)
        if not sql.retryCount or sql.retryCount < SQL_RERTY_COUNT
          sql.retryCount = (sql.retryCount or 0) + 1
          @queue.push(sql)
        else
          @err = err if err
        callback()
      )
    , 1)

  start:->
    @sourceConnection.query @sql, ((row) =>
      @rowCallback row  if @rowCallback
      @rows++
    ), ((err, data, columns, last) =>
      @printProgress()
      return @callback(err) if err
      if data.length > 0
        @queue.push({query:@destinationConnection.createSQL(columns, data, @destinationTable)})
        @queueItemsCount++
      if last
        end = =>
          @callback @err
          return
        if @queue.length() > 0
          @queue.drain = end
        else
          end()
      return
    ), 1000
    return

  printProgress:->
    if @logProgress
      stats = @stats()
      process.stdout.write "\r" + @sourceConnection.name + "->" + @destinationConnection.name + "." + @destinationTable + "(rows " + stats.numberOfRows + "; queue " + stats.queue.processed + "/" + stats.queue.count + ")"
    return

  stats: ->
    numberOfRows: @rows
    queue:
      count: @queueItemsCount
      processed: @queueItemsCount - @queue.length()


module.exports = BatchImporterTransfer
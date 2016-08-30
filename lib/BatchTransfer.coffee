async = require('async')

class BatchImporterTransfer extends Object
  constructor:(@sourceConnection, @destinationConnection, @destinationTable, @sql, @rowCallback, @callback)->
    super
    @logProgress = no
    @err = null
    @rows = 0
    @queueItemsCount = 0
    @queue = async.queue((sql, callback)=>
      @printProgress()
      @destinationConnection.query(sql, null, (err,res)=>
#        console.log('..',@sql,res)
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
        @queue.push(@destinationConnection.createSQL(columns, data, @destinationTable))
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
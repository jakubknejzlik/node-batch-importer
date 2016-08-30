async = require('async')
mysql = require('mysql')
genericPool = require('generic-pool')

BatchImporterTransfer = require('./BatchTransfer')

class BatchImporter extends Object
  constructor:()->
    super
    @connections = {}
    @waitingTransfers = []
    @runningTransfers = []
    @showTransferProgress = false


  _getConnectionType:(connectionType)->
    try
      return require('./connections/'+connectionType)
    catch e
      throw new Error('type ' + connectionType + ' not supported')

  addConnection:(name,type,settings)->
    connectionType = @_getConnectionType(type)
    @connections[name] = new (connectionType)(name,settings);

  removeConnection:(name)->
    delete @connections[name]

  getConnection:(name)->
    return @connections[name]

  fetchData:(source,sql,rowCallback,callback)->
    if not callback
      callback = rowCallback
      rowCallback = null
    sqls = sql.split(';').filter (value)->
      return value
    results = []
    async.forEach sqls,(query,cb)=>
      @connections[source].query query,rowCallback,(err,res)=>
        return cb(err) if err
        results.push(res)
        cb()
    ,(err)=>
      if results.length == 1
        results = results[0]
      callback(err,results)

  transferData:(source,destination,sql,rowCallback,callback)->
    if not callback
      callback = rowCallback
      rowCallback = null
    _destination = destination.split('.');
    destinationConnection = this.getConnection(_destination[0]);
    tableName = _destination[1];
    if destinationConnection.options.tablePrefix
      tableName = destinationConnection.options.tablePrefix + tableName
    transfer = new BatchImporterTransfer this.getConnection(source),destinationConnection,tableName,sql,rowCallback,(err)=>
      callback(err)
#      @runningTransfers.splice(@runningTransfers.indexOf(transfer),1)
#      @startNextAvailableTransferIfPossible()

    transfer.logProgress = @logTransferProgress


    transfer.start()
#    @waitingTransfers.push(transfer)
#    @startNextAvailableTransferIfPossible()

#  startNextAvailableTransferIfPossible: ()->
#    if @runningTransfers.length == 0
#      if @waitingTransfers.length == 0
#        if @logTransferProgress
#          console.log('all transfers completed')
#        return;
#      transfer = this.waitingTransfers.shift();
#      @runningTransfers.push(transfer)
#      transfer.start()

  closeConnections:()->
    for key,connection of @connections
      connection.close();

  stats: ()->
    stats = {
      transfers:{
        waiting:this.waitingTransfers.length,
        running:{
          count:this.runningTransfers.length,
          stats:[]
        }
      }
    }
    for transfer in @runningTransfers
      stats.transfers.running.stats.push(transfer.stats())
    return stats





module.exports = BatchImporter;
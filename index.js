var NedbDatastore = require('nedb'),
	thenify = require('thenify')

class Finder {
	constructor(cursor){
		this._cursor = cursor
		for(const k in cursor){
			const fn = cursor[k]
			if(typeof fn === 'function'){
				this[k] = (...args)=>{
					return new Finder(fn.call(cursor, ...args))
				}
			}
		}
	}

	then(...args){
		return new Promise((resolve, reject)=>{
			this._cursor.exec((err, docs)=>{
				if(err) return reject(err)
				resolve(docs)
			})
		}).then(...args)
	}
}

function fromInstance(nedbInstance) {
	var newDB = { nedb: nedbInstance }

	var methods = ['loadDatabase', 'insert', 'findOne', 'count', 'update', 'remove', 'ensureIndex', 'removeIndex']
	for (var i = 0; i < methods.length; ++i) {
		var m = methods[i]
		newDB[m] = thenify(nedbInstance[m].bind(nedbInstance))
	}

	newDB.find = function (...args) {
		const result = nedbInstance.find(...args)
		return new Finder(result)
	}

	newDB.cfind = function (query, projections) {
		var cursor = nedbInstance.find(query, projections)
		cursor.exec = thenify(cursor.exec.bind(cursor))
		return cursor
	}

	newDB.cfindOne = function (query, projections) {
		var cursor = nedbInstance.findOne(query, projections)
		cursor.exec = thenify(cursor.exec.bind(cursor))
		return cursor
	}

	newDB.ccount = function (query) {
		var cursor = nedbInstance.count(query)
		cursor.exec = thenify(cursor.exec.bind(cursor))
		return cursor
	}

	// added by bslee
	newDB.setAutocompactionInterval = function (interval, minimumWritten) {
		nedbInstance.persistence.setAutocompactionInterval(interval, minimumWritten)
	}

	newDB.compact = function (cb) {
		nedbInstance.persistence.compactDatafile()
		if (typeof cb === 'function') {
			nedbInstance.once('compaction.done', cb)
		}
	}

	return newDB
}

function datastore(options) {
	var nedbInstance = new NedbDatastore(options)
	return fromInstance(nedbInstance)
}

// so that import { datastore } still works:
datastore.datastore = datastore
datastore.fromInstance = fromInstance

module.exports = datastore

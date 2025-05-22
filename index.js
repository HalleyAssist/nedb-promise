const NedbDatastore = require('./nedb/datastore'),
      util = require('util'),
	  Cursor = require('./nedb/cursor')

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

const Methods = ['loadDatabase', 'insert', 'insertUnsafe', 'insertUnsafest', 'findOne', 'count', 'update', 'upsert', 'remove', 'ensureIndex', 'removeIndex', 'cleanupExpired']

function fromInstance(nedbInstance) {
	let newDB = { nedb: nedbInstance }

	for(const m of Methods){
		newDB[m] = util.promisify(nedbInstance[m].bind(nedbInstance))
	}

	newDB.find = function (...args) {
		if(args.length === 1 && args[0] instanceof Cursor){
			return new Finder(args[0])
		}
		return new Finder(nedbInstance.find(...args))
	}
	newDB.findUnsafe = function (...args) {
		return new Finder(nedbInstance.findUnsafe(...args))
	}

	newDB.cfind = function (query, projections) {
		let cursor = nedbInstance.find(query, projections)
		cursor.exec = util.promisify(cursor.exec.bind(cursor))
		return cursor
	}

	newDB.cfindOne = function (query, projections) {
		let cursor = nedbInstance.findOne(query, projections)
		cursor.exec = util.promisify(cursor.exec.bind(cursor))
		return cursor
	}

	newDB.ccount = function (query) {
		let cursor = nedbInstance.count(query)
		cursor.exec = util.promisify(cursor.exec.bind(cursor))
		return cursor
	}

	// added by bslee
	newDB.setAutocompactionInterval = function (interval, minimumWritten, minimumBytes) {
		nedbInstance.persistence.setAutocompactionInterval(interval, minimumWritten, minimumBytes)
	}

	newDB.compact = function (cb) {
		nedbInstance.persistence.compactDatafile()
		if (typeof cb === 'function') {
			nedbInstance.once('compaction.done', cb)
		}
	}

	newDB.datastore = nedbInstance

	return newDB
}

function datastore(options) {
	let nedbInstance = new NedbDatastore(options)
	return fromInstance(nedbInstance)
}

// so that import { datastore } still works:
datastore.datastore = datastore
datastore.fromInstance = fromInstance
datastore.Model = require('./nedb/model')
datastore.Cursor = require('./nedb/cursor')

module.exports = datastore

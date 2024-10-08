'use strict'

const assert = require('assert')
const co = require('co')
const datastore = require('../')
const path = require('path')
const _ = require('lodash')

describe('datastore', () => {
	const DB = datastore({ autoload: true })

	//
	// before each: clear and then stage data
	//
	beforeEach(co.wrap(function * () {
		yield DB.remove({ }, { multi: true })
		
		yield DB.insert([ {
			num: 1,
			alpha: 'a'
		}, {
			num: 2,
			alpha: 'b'
		}, {
			num: 3,
			alpha: 'c'
		} ])
	}))

	describe('#findOne()', () => {
		it('should only return one', co.wrap(function * () {
			let doc = yield DB.findOne({ num: 2 })
			assert.equal(doc.num, 2)
		}))

		it('should project', co.wrap(function * () {
			let doc = yield DB.cfindOne({ num: 2 }, true)
				.projection({ num: 1, _id: false })
				.exec()

			assert.equal(doc.num, 2)
			assert.equal(doc.alpha, undefined)
		}))
	})

	describe('#count()', () => {
		it('should return the number of documents in the database', co.wrap(function * () {
			let count = yield DB.count({})
			assert.equal(count, 3)
		}))

		it('should work with cursors', co.wrap(function * () {
			const count = yield DB.ccount({}).limit(2).exec()
			assert.equal(count, 2)
		}))
	})

	describe('#insert()', () => {
		it('should insert two documents', co.wrap(function * () {
			let beforeCount = yield DB.count({})
			let docs = yield DB.insert([{
				num: 4,
				alpha: 'd'
			}, {
				num: 5,
				alpha: 'e'
			}])
			let afterCount = yield DB.count({})

			assert.equal(afterCount - beforeCount, 2)
		}))
	})

	describe('#update()', () => {
		it('should update a document', co.wrap(function * () {
			yield DB.update({ num: 3 }, { $set: { updated: true } })
			let updated = yield DB.findOne({ num: 3 })
			assert(updated.updated)
		}))
	})

	describe('#remove()', () => {
		it('should remove a document', co.wrap(function * () {
			let beforeCount = yield DB.count({})
			yield DB.remove({})
			let afterCount = yield DB.count({})

			assert.equal(beforeCount - afterCount, 1)
		}))

		it('should remove all documents', co.wrap(function * () {
			yield DB.remove({}, { multi: true })
			let afterCount = yield DB.count({})

			assert.equal(afterCount, 0)
		}))
	})

	describe('Cursors', () => {
		it('should work with no suffix', co.wrap(function * () {
			const db = datastore('')
			yield db.insert([ { id: 2, name: 'Tim' }, { id: 1, name: 'Tom' } ])
			const res = yield db.cfind().sort({ name: -1 }).projection({ name: 1 }).exec()

			assert.equal(res[0].name, 'Tom')
			assert.equal(res[1].name, 'Tim')
			assert.equal(typeof res[0].id, 'undefined')
		}))
	})
})

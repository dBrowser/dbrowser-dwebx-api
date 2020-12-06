const test = require('ava')
const tutil = require('./util')
const dba = require('../index')

var daemon

test.before(async () => {
  daemon = await tutil.createOneDaemon()
})
test.after(async () => {
  await daemon.cleanup()
})

test('unlink', async t => {
  var archive = await tutil.createArchive(daemon, [
    'a',
    'b/',
    'b/a',
    'c/',
    'c/b/',
    'c/b/a'
  ])

  await dba.unlink(archive, '/a')
  await t.throws(dba.stat(archive, '/a'))
  await dba.unlink(archive, 'b/a')
  await t.throws(dba.stat(archive, 'b/a'))
  await dba.unlink(archive, '/c/b/a')
  await t.throws(dba.stat(archive, '/c/b/a'))
  t.deepEqual((await dba.readdir(archive, '/', {recursive: true})).sort().map(tutil.tonix), ['b', 'c', 'c/b'].sort())
})

test('unlink NotFoundError, NotAFileError', async t => {
  var archive = await tutil.createArchive(daemon, [
    'a',
    'b/',
    'b/a',
    'c/',
    'c/b/',
    'c/b/a'
  ])

  const err1 = await t.throws(dba.unlink(archive, '/bar'))
  t.truthy(err1.notFound)
  const err2 = await t.throws(dba.unlink(archive, '/b'))
  t.truthy(err2.notAFile)
})

test('rmdir', async t => {
  var archive = await tutil.createArchive(daemon, [
    'a',
    'b/',
    'b/a/',
    'c/',
    'c/b/'
  ])

  await dba.rmdir(archive, 'b/a')
  await dba.rmdir(archive, 'b')
  await dba.rmdir(archive, 'c/b')
  t.deepEqual((await dba.readdir(archive, '/', {recursive: true})).sort(), ['a', 'c'].sort())
})

test('rmdir recursive', async t => {
  var archive = await tutil.createArchive(daemon, [
    'a',
    'b/',
    'b/a/',
    'b/b',
    'b/c',
    'b/d/',
    'b/d/a',
    'b/d/b',
    'b/d/c/',
    'b/d/c/a',
    'b/d/c/b',
    'b/d/d',
    'c/',
    'c/b/'
  ])

  await dba.rmdir(archive, 'b', {recursive: true})
  t.deepEqual((await dba.readdir(archive, '/', {recursive: true})).map(tutil.tonix).sort(), ['a', 'c', 'c/b'].sort())
})

test('rmdir recursive w/mounts', async t => {
  var archive = await tutil.createArchive(daemon, [
    'foo',
    'sub/'
  ])
  var archive2 = await tutil.createArchive(daemon, [
    'mountfile',
    'mountdir/'
  ])
  await dba.mount(archive, '/sub/mount', archive2.key)

  await dba.rmdir(archive, 'sub', {recursive: true})
  t.deepEqual((await dba.readdir(archive, '/', {recursive: true})).map(tutil.tonix).sort(), ['foo'].sort())
  t.deepEqual((await dba.readdir(archive2, '/', {recursive: true})).map(tutil.tonix).sort(), ['mountfile', 'mountdir'].sort())
})

test('rmdir NotFoundError, NotAFolderError, DestDirectoryNotEmpty', async t => {
  var archive = await tutil.createArchive(daemon, [
    'a',
    'b/',
    'b/a/',
    'c/',
    'c/b/'
  ])

  const err1 = await t.throws(dba.rmdir(archive, '/bar'))
  t.truthy(err1.notFound)
  const err2 = await t.throws(dba.rmdir(archive, '/a'))
  t.truthy(err2.notAFolder)
  const err3 = await t.throws(dba.rmdir(archive, '/b'))
  t.truthy(err3.destDirectoryNotEmpty)
})

test('ArchiveNotWritableError', async t => {
  var archive = await tutil.createArchive(daemon, [], tutil.FAKE_DAT_KEY)

  const err1 = await t.throws(dba.unlink(archive, '/bar'))
  t.truthy(err1.archiveNotWritable)
  const err2 = await t.throws(dba.rmdir(archive, '/bar'))
  t.truthy(err2.archiveNotWritable)
})

test.skip('unlink w/fs', async t => {
  var fs = await tutil.createFs([
    'a',
    'b/',
    'b/a',
    'c/',
    'c/b/',
    'c/b/a'
  ])

  await dba.unlink(fs, '/a')
  await t.throws(dba.stat(fs, '/a'))
  await dba.unlink(fs, 'b/a')
  await t.throws(dba.stat(fs, 'b/a'))
  await dba.unlink(fs, '/c/b/a')
  await t.throws(dba.stat(fs, '/c/b/a'))
  t.deepEqual((await dba.readdir(fs, '/', {recursive: true})).sort().map(tutil.tonix), ['b', 'c', 'c/b'])
})

test.skip('unlink NotFoundError, NotAFileError w/fs', async t => {
  var fs = await tutil.createFs([
    'a',
    'b/',
    'b/a',
    'c/',
    'c/b/',
    'c/b/a'
  ])

  const err1 = await t.throws(dba.unlink(fs, '/bar'))
  t.truthy(err1.notFound)
  const err2 = await t.throws(dba.unlink(fs, '/b'))
  t.truthy(err2.notAFile)
})

test.skip('rmdir w/fs', async t => {
  var fs = await tutil.createFs([
    'a',
    'b/',
    'b/a/',
    'c/',
    'c/b/'
  ])

  await dba.rmdir(fs, 'b/a')
  await dba.rmdir(fs, 'b')
  await dba.rmdir(fs, 'c/b')
  t.deepEqual((await dba.readdir(fs, '/', {recursive: true})).sort(), ['a', 'c'].sort())
})

test.skip('rmdir recursive w/fs', async t => {
  var fs = await tutil.createFs([
    'a',
    'b/',
    'b/a/',
    'b/b',
    'b/c',
    'b/d/',
    'b/d/a',
    'b/d/b',
    'b/d/c/',
    'b/d/c/a',
    'b/d/c/b',
    'b/d/d',
    'c/',
    'c/b/'
  ])

  await dba.rmdir(fs, 'b', {recursive: true})
  t.deepEqual((await dba.readdir(fs, '/', {recursive: true})).map(tutil.tonix).sort(), ['a', 'c', 'c/b'])
})

test.skip('rmdir NotFoundError, NotAFolderError, DestDirectoryNotEmpty w/fs', async t => {
  var fs = await tutil.createFs([
    'a',
    'b/',
    'b/a/',
    'c/',
    'c/b/'
  ])

  const err1 = await t.throws(dba.rmdir(fs, '/bar'))
  t.truthy(err1.notFound)
  const err2 = await t.throws(dba.rmdir(fs, '/a'))
  t.truthy(err2.notAFolder)
  const err3 = await t.throws(dba.rmdir(fs, '/b'))
  t.truthy(err3.destDirectoryNotEmpty)
})

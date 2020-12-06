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

test('mount and unmount', async t => {
  var archive1 = await tutil.createArchive(daemon, [])
  var archive2 = await tutil.createArchive(daemon, [
    'bar'
  ])

  await dba.mount(archive1, '/foo', archive2.key)
  t.deepEqual((await dba.readdir(archive1, '/')).sort(), ['foo'].sort())
  t.deepEqual((await dba.readdir(archive1, '/foo')).sort(), ['bar'].sort())
  t.deepEqual((await dba.stat(archive1, '/foo')).isDirectory(), true)
  await dba.writeFile(archive2, 'hello.txt', 'hello')
  t.deepEqual(await dba.readFile(archive1, '/foo/hello.txt', 'utf8'), 'hello')

  await dba.unmount(archive1, '/foo')
  t.deepEqual((await dba.readdir(archive1, '/')).sort(), [])

  await dba.mount(archive1, '/bar/foo', archive2.key)
  t.deepEqual((await dba.readdir(archive1, '/bar/foo')).sort(), ['bar', 'hello.txt'].sort())
})

test('mount at version', async t => {
  var archive1 = await tutil.createArchive(daemon, [])
  var archive2 = await tutil.createArchive(daemon, [
    'bar'
  ])

  dba.writeFile(archive2, '/test.txt', '1')
  dba.writeFile(archive2, '/test.txt', '2')

  await dba.mount(archive1, '/foo', {key: archive2.key, version: 3})
  t.deepEqual(await dba.readFile(archive1, '/foo/test.txt', 'utf8'), '1')
  await dba.mount(archive1, '/foo2', {key: archive2.key, version: 4})
  t.deepEqual(await dba.readFile(archive1, '/foo2/test.txt', 'utf8'), '2')
})

test('EntryAlreadyExistsError', async t => {
  var archive = await tutil.createArchive(daemon, [])
  var archive2 = await tutil.createArchive(daemon, [])

  await dba.writeFile(archive, '/file', 'new content')
  const err = await t.throws(dba.mount(archive, '/file', archive2.key))
  t.truthy(err.entryAlreadyExists)
})

test('ArchiveNotWritableError', async t => {
  var archive = await tutil.createArchive(daemon, [], tutil.FAKE_DAT_KEY)
  var archive2 = await tutil.createArchive(daemon, [])

  const err = await t.throws(dba.mount(archive, '/bar', archive2.key))
  t.truthy(err.archiveNotWritable)

  const err2 = await t.throws(dba.unmount(archive, '/bar'))
  t.truthy(err2.archiveNotWritable)
})

test('InvalidPathError', async t => {
  var archive = await tutil.createArchive(daemon, [])
  var archive2 = await tutil.createArchive(daemon, [])

  const err = await t.throws(dba.mount(archive, '/foo%20bar', archive2.key))
  t.truthy(err.invalidPath)

  const err2 = await t.throws(dba.unmount(archive, '/foo%20bar'))
  t.truthy(err2.invalidPath)
})

test('unmount NotFoundError', async t => {
  var archive = await tutil.createArchive(daemon, [])

  const err = await t.throws(dba.unmount(archive, '/foo/bar'))
  t.truthy(err.notFound)
})

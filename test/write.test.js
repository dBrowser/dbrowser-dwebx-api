const test = require('ava')
const pump = require('pump')
const intoStream = require('into-stream')
const tutil = require('./util')
const dba = require('../index')

var daemon

test.before(async () => {
  daemon = await tutil.createOneDaemon()
})
test.after(async () => {
  await daemon.cleanup()
})

test('writeFile', async t => {
  var archive = await tutil.createArchive(daemon, [
    'foo'
  ])

  t.deepEqual(await dba.readFile(archive, 'foo'), 'content')
  await dba.writeFile(archive, '/foo', 'new content')
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  await dba.writeFile(archive, 'foo', Buffer.from([0x01]))
  t.deepEqual(await dba.readFile(archive, 'foo', 'buffer'), Buffer.from([0x01]))
  await dba.writeFile(archive, 'foo', '02', 'hex')
  t.deepEqual(await dba.readFile(archive, 'foo', 'buffer'), Buffer.from([0x02]))
  await dba.writeFile(archive, 'foo', 'Aw==', { encoding: 'base64' })
  t.deepEqual(await dba.readFile(archive, 'foo', 'buffer'), Buffer.from([0x03]))
  await dba.writeFile(archive, 'foo.json', {hello: 'world'}, { encoding: 'json' })
  t.deepEqual(await dba.readFile(archive, 'foo.json', 'json'), {hello: 'world'})

  await dba.writeFile(archive, '/one/two/three.txt', 'asdf')
  t.deepEqual((await dba.stat(archive, '/one')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two/three.txt')).isFile(), true)
  await dba.writeFile(archive, '/one/two/four.txt', 'asdf')
  t.deepEqual((await dba.stat(archive, '/one')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two/three.txt')).isFile(), true)
  t.deepEqual((await dba.stat(archive, '/one/two/four.txt')).isFile(), true)

  await dba.writeFile(archive, '/a-file.txt', 'asdf')
  await dba.writeFile(archive, '/a-file.txt/sub-file.txt', 'fdsa')
  t.deepEqual(await dba.readFile(archive, '/a-file.txt'), 'asdf')
  t.truthy((await dba.stat(archive, '/a-file.txt')).isFile())
  t.deepEqual(await dba.readdir(archive, '/a-file.txt'), ['sub-file.txt'])
  t.deepEqual(await dba.readFile(archive, '/a-file.txt/sub-file.txt'), 'fdsa')
})

test('writeFile preserves ctime and metadata', async t => {
  var archive = await tutil.createArchive(daemon, [])

  await dba.writeFile(archive, '/foo.txt', 'bar', {metadata: {hello: 'world'}})
  var stat1 = await dba.stat(archive, '/foo.txt')
  await dba.writeFile(archive, '/foo.txt', 'baz')
  var stat2 = await dba.stat(archive, '/foo.txt')
  t.deepEqual(stat1.ctime, stat2.ctime)
  t.deepEqual(stat1.metadata, stat2.metadata)
})

test.skip('writeFile w/fs', async t => {
  var fs = await tutil.createFs([
    'foo'
  ])

  t.deepEqual(await dba.readFile(fs, 'foo'), 'content')
  await dba.writeFile(fs, '/foo', 'new content')
  t.deepEqual(await dba.readFile(fs, 'foo'), 'new content')
  await dba.writeFile(fs, 'foo', Buffer.from([0x01]))
  t.deepEqual(await dba.readFile(fs, 'foo', 'buffer'), Buffer.from([0x01]))
  await dba.writeFile(fs, 'foo', '02', 'hex')
  t.deepEqual(await dba.readFile(fs, 'foo', 'buffer'), Buffer.from([0x02]))
  await dba.writeFile(fs, 'foo', 'Aw==', { encoding: 'base64' })
  t.deepEqual(await dba.readFile(fs, 'foo', 'buffer'), Buffer.from([0x03]))
})

test('mkdir', async t => {
  var archive = await tutil.createArchive(daemon, [
    'foo'
  ])

  await dba.mkdir(archive, '/bar')
  t.deepEqual((await dba.readdir(archive, '/')).sort(), ['bar', 'foo'].sort())
  t.deepEqual((await dba.stat(archive, '/bar')).isDirectory(), true)

  await dba.mkdir(archive, '/one/two/three')
  t.deepEqual((await dba.stat(archive, '/one')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two/three')).isDirectory(), true)
  await dba.mkdir(archive, '/one/two/three/four')
  t.deepEqual((await dba.stat(archive, '/one')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two/three')).isDirectory(), true)
  t.deepEqual((await dba.stat(archive, '/one/two/three/four')).isDirectory(), true)

  await dba.writeFile(archive, '/foo.txt', 'bar')
  await dba.mkdir(archive, '/foo.txt/bar')
  t.deepEqual((await dba.stat(archive, '/foo.txt')).isFile(), true)
  t.deepEqual((await dba.stat(archive, '/foo.txt/bar')).isDirectory(), true)
})

test.skip('mkdir w/fs', async t => {
  var fs = await tutil.createFs([
    'foo'
  ])

  await dba.mkdir(fs, '/bar')
  t.deepEqual((await dba.readdir(fs, '/')).sort(), ['bar', 'foo'])
  t.deepEqual((await dba.stat(fs, '/bar')).isDirectory(), true)
})

test('symlink', async t => {
  var archive = await tutil.createArchive(daemon, [
    'foo',
    'bar/',
    'bar/one',
    'bar/two'
  ])

  await dba.symlink(archive, '/foo', '/foo2')
  await dba.symlink(archive, '/bar', '/bar2')
  t.deepEqual((await dba.readdir(archive, '/bar2')).sort(), ['one', 'two'].sort())
  t.deepEqual((await dba.stat(archive, '/bar2')).isDirectory(), true)
  // t.deepEqual((await dba.lstat(archive, '/bar2')).isSymbolicLink(), true) TODO
  t.deepEqual((await dba.readFile(archive, '/foo2')), 'content')
  t.deepEqual((await dba.stat(archive, '/foo2')).isFile(), true)
  // t.deepEqual((await dba.lstat(archive, '/foo2')).isSymbolicLink(), true) TODO
})

test.skip('symlink w/fs', async t => {
  var fs = await tutil.createFs([
    'foo',
    'bar/',
    'bar/one',
    'bar/two'
  ])

  await dba.symlink(fs, '/foo', '/foo2')
  await dba.symlink(fs, '/bar', '/bar2')
  t.deepEqual((await dba.readdir(fs, '/bar2')).sort(), ['one', 'two'].sort())
  t.deepEqual((await dba.stat(fs, '/bar2')).isDirectory(), true)
  t.deepEqual((await dba.readFile(fs, '/foo2')), 'content')
  t.deepEqual((await dba.stat(fs, '/foo2')).isFile(), true)
})

test('copy', async t => {
  var archive = await tutil.createArchive(daemon, [
    {name: 'a', content: 'thecopy'},
    'b/',
    'b/a',
    'b/b/',
    'b/b/a',
    'b/b/b',
    'b/b/c',
    'b/c',
    'c/'
  ])
  await dba.updateMetadata(archive, 'a', {foo: 'bar'})
  await dba.symlink(archive, '/a', '/d')

  await dba.copy(archive, '/a', archive, '/a-copy')
  t.deepEqual(await dba.readFile(archive, '/a-copy'), 'thecopy')
  t.deepEqual((await dba.stat(archive, '/a-copy')).isFile(), true)
  t.deepEqual((await dba.stat(archive, '/a-copy')).metadata.foo, 'bar')

  await dba.copy(archive, '/d', archive, '/d-copy')
  t.is((await dba.stat(archive, '/d-copy')).isFile(), true)
  t.is((await dba.stat(archive, '/d-copy', {lstat: true})).linkname, '/a')

  await dba.copy(archive, '/b', archive, '/b-copy')
  t.deepEqual((await dba.stat(archive, '/b-copy')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive, '/b-copy/a'), 'content')
  t.deepEqual((await dba.stat(archive, '/b-copy/b')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive, '/b-copy/b/a'), 'content')
  t.deepEqual(await dba.readFile(archive, '/b-copy/b/b'), 'content')
  t.deepEqual(await dba.readFile(archive, '/b-copy/b/c'), 'content')
  t.deepEqual(await dba.readFile(archive, '/b-copy/c'), 'content')

  await dba.copy(archive, '/b/b', archive, '/c')
  t.deepEqual((await dba.stat(archive, '/c')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive, 'c/a'), 'content')
  t.deepEqual(await dba.readFile(archive, 'c/b'), 'content')
  t.deepEqual(await dba.readFile(archive, 'c/c'), 'content')

  const err1 = await t.throws(dba.copy(archive, '/b', archive, '/b/sub'))
  t.truthy(err1.invalidPath)

  const err2 = await t.throws(dba.copy(archive, '/b', archive, '/b'))
  t.truthy(err2.invalidPath)
})

test('copy between archives', async t => {
  var archive1 = await tutil.createArchive(daemon, [
    {name: 'a', content: 'thecopy'},
    'b/',
    'b/a',
    'b/b/',
    'b/b/a',
    'b/b/b',
    'b/b/c',
    'b/c',
    'c/'
  ])
  await dba.updateMetadata(archive1, 'a', {foo: 'bar'})
  await dba.symlink(archive1, '/a', '/d')
  var archive2 = await tutil.createArchive(daemon, [])

  await dba.copy(archive1, '/a', archive2, '/a')
  t.deepEqual(await dba.readFile(archive2, '/a'), 'thecopy')
  t.deepEqual((await dba.stat(archive2, '/a')).isFile(), true)
  t.deepEqual((await dba.stat(archive2, '/a')).metadata.foo, 'bar')

  await dba.copy(archive1, '/d', archive2, '/d')
  t.is((await dba.stat(archive2, '/d')).isFile(), true)
  t.is((await dba.stat(archive2, '/d', {lstat: true})).linkname, '/a')

  await dba.copy(archive1, '/b', archive2, '/b')
  t.deepEqual((await dba.stat(archive2, '/b')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive2, '/b/a'), 'content')
  t.deepEqual((await dba.stat(archive2, '/b/b')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive2, '/b/b/a'), 'content')
  t.deepEqual(await dba.readFile(archive2, '/b/b/b'), 'content')
  t.deepEqual(await dba.readFile(archive2, '/b/b/c'), 'content')
  t.deepEqual(await dba.readFile(archive2, '/b/c'), 'content')

  await dba.copy(archive1, '/b/b', archive2, '/c')
  t.deepEqual((await dba.stat(archive2, '/c')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive2, 'c/a'), 'content')
  t.deepEqual(await dba.readFile(archive2, 'c/b'), 'content')
  t.deepEqual(await dba.readFile(archive2, 'c/c'), 'content')
})

test('copy with mounts', async t => {
  var archive1 = await tutil.createArchive(daemon, [
    'foo',
    'subdir/'
  ])
  var archive2 = await tutil.createArchive(daemon, [
    'mountfile',
    'mountdir/'
  ])
  await dba.mount(archive1, '/subdir/mount', archive2.key)

  await dba.copy(archive1, '/subdir', archive1, '/subdir-copy')
  t.deepEqual(
    (await dba.readdir(archive1, '/subdir', {recursive: true})).sort(),
    (await dba.readdir(archive1, '/subdir-copy', {recursive: true})).sort()
  )
})

test.skip('copy w/fs', async t => {
  var fs = await tutil.createFs([
    {name: 'a', content: 'thecopy'},
    'b/',
    'b/a',
    'b/b/',
    'b/b/a',
    'b/b/b',
    'b/b/c',
    'b/c',
    'c/'
  ])

  await dba.copy(fs, '/a', fs, '/a-copy')
  t.deepEqual(await dba.readFile(fs, '/a-copy'), 'thecopy')
  t.deepEqual((await dba.stat(fs, '/a-copy')).isFile(), true)

  await dba.copy(fs, '/b', fs, '/b-copy')
  t.deepEqual((await dba.stat(fs, '/b-copy')).isDirectory(), true)
  t.deepEqual(await dba.readFile(fs, '/b-copy/a'), 'content')
  t.deepEqual((await dba.stat(fs, '/b-copy/b')).isDirectory(), true)
  t.deepEqual(await dba.readFile(fs, '/b-copy/b/a'), 'content')
  t.deepEqual(await dba.readFile(fs, '/b-copy/b/b'), 'content')
  t.deepEqual(await dba.readFile(fs, '/b-copy/b/c'), 'content')
  t.deepEqual(await dba.readFile(fs, '/b-copy/c'), 'content')

  await dba.copy(fs, '/b/b', fs, '/c')
  t.deepEqual((await dba.stat(fs, '/c')).isDirectory(), true)
  t.deepEqual(await dba.readFile(fs, 'c/a'), 'content')
  t.deepEqual(await dba.readFile(fs, 'c/b'), 'content')
  t.deepEqual(await dba.readFile(fs, 'c/c'), 'content')
})

test('rename', async t => {
  var archive = await tutil.createArchive(daemon, [
    'a',
    'b/',
    'b/a',
    'b/b/',
    'b/b/a',
    'b/b/b',
    'b/b/c',
    'b/c',
    'c/'
  ])
  await dba.updateMetadata(archive, 'a', {foo: 'bar'})

  await dba.rename(archive, '/a', archive, '/a-rename')
  t.deepEqual(await dba.readFile(archive, '/a-rename'), 'content')
  t.deepEqual((await dba.stat(archive, '/a-rename')).isFile(), true)
  t.deepEqual((await dba.stat(archive, '/a-rename')).metadata.foo, 'bar')

  await dba.symlink(archive, '/a-rename', '/d')
  await dba.rename(archive, '/d', archive, '/d-rename')
  t.deepEqual((await dba.stat(archive, '/d-rename')).isFile(), true)
  t.deepEqual((await dba.stat(archive, '/d-rename', {lstat: true})).linkname, '/a-rename')

  await dba.rename(archive, '/b', archive, '/b-rename')
  t.deepEqual((await dba.stat(archive, '/b-rename')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive, '/b-rename/a'), 'content')
  t.deepEqual((await dba.stat(archive, '/b-rename/b')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive, '/b-rename/b/a'), 'content')
  t.deepEqual(await dba.readFile(archive, '/b-rename/b/b'), 'content')
  t.deepEqual(await dba.readFile(archive, '/b-rename/b/c'), 'content')
  t.deepEqual(await dba.readFile(archive, '/b-rename/c'), 'content')

  await dba.rename(archive, '/b-rename/b', archive, '/c/newb')
  t.deepEqual((await dba.stat(archive, '/c/newb')).isDirectory(), true)
  t.deepEqual(await dba.readFile(archive, 'c/newb/a'), 'content')
  t.deepEqual(await dba.readFile(archive, 'c/newb/b'), 'content')
  t.deepEqual(await dba.readFile(archive, 'c/newb/c'), 'content')

  const err1 = await t.throws(dba.rename(archive, '/b-rename', archive, '/b-rename/sub'))
  t.truthy(err1.invalidPath)
})

test('rename with mounts', async t => {
  var archive1 = await tutil.createArchive(daemon, [
    'foo',
    'subdir/'
  ])
  var archive2 = await tutil.createArchive(daemon, [
    'mountfile',
    'mountdir/'
  ])
  await dba.mount(archive1, '/subdir/mount', archive2.key)

  await dba.rename(archive1, '/subdir', archive1, '/subdir-renamed')
  t.deepEqual(
    (await dba.readdir(archive1, '/subdir-renamed')).sort(),
    ['mount']
  )
  t.deepEqual(
    (await dba.readdir(archive1, '/subdir-renamed/mount')).sort(),
    ['mountdir', 'mountfile']
  )
})

test.skip('rename w/fs', async t => {
  var fs = await tutil.createFs([
    'a',
    'b/',
    'b/a',
    'b/b/',
    'b/b/a',
    'b/b/b',
    'b/b/c',
    'b/c',
    'c/'
  ])

  await dba.rename(fs, '/a', fs, '/a-rename')
  t.deepEqual(await dba.readFile(fs, '/a-rename'), 'content')
  t.deepEqual((await dba.stat(fs, '/a-rename')).isFile(), true)

  await dba.rename(fs, '/b', fs, '/b-rename')
  t.deepEqual((await dba.stat(fs, '/b-rename')).isDirectory(), true)
  t.deepEqual(await dba.readFile(fs, '/b-rename/a'), 'content')
  t.deepEqual((await dba.stat(fs, '/b-rename/b')).isDirectory(), true)
  t.deepEqual(await dba.readFile(fs, '/b-rename/b/a'), 'content')
  t.deepEqual(await dba.readFile(fs, '/b-rename/b/b'), 'content')
  t.deepEqual(await dba.readFile(fs, '/b-rename/b/c'), 'content')
  t.deepEqual(await dba.readFile(fs, '/b-rename/c'), 'content')

  await dba.rename(fs, '/b-rename/b', fs, '/c/newb')
  t.deepEqual((await dba.stat(fs, '/c/newb')).isDirectory(), true)
  t.deepEqual(await dba.readFile(fs, 'c/newb/a'), 'content')
  t.deepEqual(await dba.readFile(fs, 'c/newb/b'), 'content')
  t.deepEqual(await dba.readFile(fs, 'c/newb/c'), 'content')
})

test('EntryAlreadyExistsError', async t => {
  var archive = await tutil.createArchive(daemon, [])

  await dba.mkdir(archive, '/dir')
  const err1 = await t.throws(dba.writeFile(archive, '/dir', 'new content'))
  t.truthy(err1.entryAlreadyExists)

  await dba.writeFile(archive, '/file', 'new content')
  const err2 = await t.throws(dba.mkdir(archive, '/file'))
  t.truthy(err2.entryAlreadyExists)

  const err3 = await t.throws(dba.copy(archive, '/dir', archive, '/file'))
  t.truthy(err3.entryAlreadyExists)

  const err4 = await t.throws(dba.copy(archive, '/file', archive, '/dir'))
  t.truthy(err4.entryAlreadyExists)

  const err5 = await t.throws(dba.rename(archive, '/file', archive, '/dir'))
  t.truthy(err5.entryAlreadyExists)
})

// test('EntryAlreadyExistsError w/fs', async t => {
//   var fs = await tutil.createFs([])

//   await dba.mkdir(fs, '/dir')
//   const err1 = await t.throws(dba.writeFile(fs, '/dir', 'new content'))
//   t.truthy(err1.entryAlreadyExists)

//   await dba.writeFile(fs, '/file', 'new content')
//   const err2 = await t.throws(dba.mkdir(fs, '/file'))
//   t.truthy(err2.entryAlreadyExists)

//   const err3 = await t.throws(dba.copy(fs, '/dir', fs, '/file'))
//   t.truthy(err3.entryAlreadyExists)

//   const err4 = await t.throws(dba.copy(fs, '/file', fs, '/dir'))
//   t.truthy(err4.entryAlreadyExists)

//   const err5 = await t.throws(dba.rename(fs, '/file', fs, '/dir'))
//   t.truthy(err5.entryAlreadyExists)
// })

test.skip('ArchiveNotWritableError', async t => {
  // FIXME
  // the 2 daemons need to communicate for this test to work
  // -prfs

  var archiveOrig = await tutil.createArchive(daemon2, [])
  var archive = await tutil.createArchive(daemon, [], archiveOrig.key)

  const err1 = await t.throws(dba.mkdir(archive, '/bar'))
  t.truthy(err1.archiveNotWritable)

  const err2 = await t.throws(dba.writeFile(archive, '/bar', 'foo'))
  t.truthy(err2.archiveNotWritable)

  const err3 = await t.throws(dba.copy(archive, '/foo', archive, '/bar'))
  t.truthy(err3.archiveNotWritable)

  const err4 = await t.throws(dba.rename(archive, '/foo', archive, '/bar'))
  t.truthy(err4.archiveNotWritable)
})

test('InvalidPathError', async t => {
  var archive = await tutil.createArchive(daemon, [])

  const err1 = await t.throws(dba.writeFile(archive, '/foo%20bar', 'new content'))
  t.truthy(err1.invalidPath)

  const err2 = await t.throws(dba.mkdir(archive, '/foo%20bar'))
  t.truthy(err2.invalidPath)

  const err3 = await t.throws(dba.copy(archive, '/foo', archive, '/foo%20bar'))
  t.truthy(err3.invalidPath)

  const err4 = await t.throws(dba.rename(archive, '/foo', archive, '/foo%20bar'))
  t.truthy(err4.invalidPath)

  const noerr = await dba.mkdir(archive, '/foo bar')
  t.truthy(typeof noerr === 'undefined')
})

test.skip('InvalidPathError w/fs', async t => {
  var fs = await tutil.createFs([])

  const err1 = await t.throws(dba.writeFile(fs, '/foo%20bar', 'new content'))
  t.truthy(err1.invalidPath)

  const err2 = await t.throws(dba.mkdir(fs, '/foo%20bar'))
  t.truthy(err2.invalidPath)

  const err3 = await t.throws(dba.copy(fs, '/foo', fs, '/foo%20bar'))
  t.truthy(err3.invalidPath)

  const err4 = await t.throws(dba.rename(fs, '/foo', fs, '/foo%20bar'))
  t.truthy(err4.invalidPath)

  const noerr = await dba.mkdir(fs, '/foo bar')
  t.truthy(typeof noerr === 'undefined')
})

async function doWriteStream (archive, path, data, opts) {
  var ws = await dba.createWriteStream(archive, path, opts)
  return new Promise((resolve, reject) => 
    pump(
      intoStream(data),
      ws,
      err => {
        if (err) reject(err)
        else resolve()
      }
    )
  )
}

test('createWriteStream', async t => {
  var archive = await tutil.createArchive(daemon, [
    'foo'
  ])

  t.deepEqual(await dba.readFile(archive, 'foo'), 'content')
  await doWriteStream(archive, '/foo', 'new content')
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  await doWriteStream(archive, 'foo', Buffer.from([0x01]))
  t.deepEqual(await dba.readFile(archive, 'foo', 'buffer'), Buffer.from([0x01]))
})

test.skip('createWriteStream w/fs', async t => {
  var fs = await tutil.createFs([
    'foo'
  ])

  t.deepEqual(await dba.readFile(fs, 'foo'), 'content')
  await doWriteStream(fs, '/foo', 'new content')
  t.deepEqual(await dba.readFile(fs, 'foo'), 'new content')
  await doWriteStream(fs, 'foo', Buffer.from([0x01]))
  t.deepEqual(await dba.readFile(fs, 'foo', 'buffer'), Buffer.from([0x01]))
})

test('read/write metadata', async t => {
  var archive = await tutil.createArchive(daemon, [])

  await dba.writeFile(archive, '/foo', 'new content')
  await dba.updateMetadata(archive, '/foo', {foo: 'bar'})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {foo: 'bar'})
  await dba.updateMetadata(archive, '/foo', {foo: 'baz'})
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {foo: 'baz'})
  await dba.updateMetadata(archive, '/foo', {stuff: 'hey', cool: 'things'})
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {foo: 'baz', stuff: 'hey', cool: 'things'})
  await dba.deleteMetadata(archive, '/foo', 'foo')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {stuff: 'hey', cool: 'things'})
  await dba.deleteMetadata(archive, '/foo', ['stuff', 'other'])
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {cool: 'things'})
})

test('binary metadata', async t => {
  var archive = await tutil.createArchive(daemon, [])

  await dba.writeFile(archive, '/foo', 'new content')
  await dba.updateMetadata(archive, '/foo', {'bin:foo': Buffer.from([1,2,3,4])})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {'bin:foo': Buffer.from([1,2,3,4])})
})

test('write metadata with file-write', async t => {
  var archive = await tutil.createArchive(daemon, [])

  await doWriteStream(archive, '/foo', 'new content', {metadata: {foo: 'bar'}})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {foo: 'bar'})
  await doWriteStream(archive, '/foo', 'new content', {metadata: {foo: 'baz', stuff: undefined}})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {foo: 'baz'})
  await doWriteStream(archive, '/foo', 'new content', {metadata: {stuff: 'hey', cool: 'things'}})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {stuff: 'hey', cool: 'things'})

  await dba.writeFile(archive, '/foo', 'new content', {metadata: {foo: 'bar'}})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {foo: 'bar'})
  await dba.writeFile(archive, '/foo', 'new content', {metadata: {foo: 'baz'}})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {foo: 'baz'})
  await dba.writeFile(archive, '/foo', 'new content', {metadata: {stuff: 'hey', cool: 'things'}})
  t.deepEqual(await dba.readFile(archive, 'foo'), 'new content')
  t.deepEqual((await dba.stat(archive, 'foo')).metadata, {stuff: 'hey', cool: 'things'})
})
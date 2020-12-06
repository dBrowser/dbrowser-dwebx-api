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

test('read/write/update manifest', async t => {
  var archive = await tutil.createArchive(daemon, [])

  await dba.writeManifest(archive, {
    url: `dwebx://${tutil.FAKE_DAT_KEY}`,
    title: 'My DWebX',
    description: 'This dwebx has a manifest!',
    type: 'foo bar',
    links: {repository: 'https://github.com/distributedweb/dbrowser-api.git'},
    author: 'dwebx://ffffffffffffffffffffffffffffffff'
  })

  t.deepEqual(await dba.readManifest(archive), {
    title: 'My DWebX',
    description: 'This dwebx has a manifest!',
    type: 'foo bar',
    links: {repository: [{href: 'https://github.com/distributedweb/dbrowser-api.git'}]},
    url: `dwebx://${tutil.FAKE_DAT_KEY}`,
    author: 'dwebx://ffffffffffffffffffffffffffffffff'
  })

  await dba.updateManifest(archive, {
    title: 'My DWebX!!',
    type: 'foo'
  })

  t.deepEqual(await dba.readManifest(archive), {
    title: 'My DWebX!!',
    description: 'This dwebx has a manifest!',
    type: 'foo',
    links: {repository: [{href: 'https://github.com/distributedweb/dbrowser-api.git'}]},
    url: `dwebx://${tutil.FAKE_DAT_KEY}`,
    author: 'dwebx://ffffffffffffffffffffffffffffffff'
  })

  await dba.updateManifest(archive, {
    author: {url: 'dwebx://foo.com'}
  })

  t.deepEqual(await dba.readManifest(archive), {
    title: 'My DWebX!!',
    description: 'This dwebx has a manifest!',
    type: 'foo',
    links: {repository: [{href: 'https://github.com/distributedweb/dbrowser-api.git'}]},
    url: `dwebx://${tutil.FAKE_DAT_KEY}`,
    author: 'dwebx://foo.com'
  })

  // should ignore bad well-known values
  // but leave others alone
  await dba.updateManifest(archive, {
    author: true,
    foobar: true
  })

  t.deepEqual(await dba.readManifest(archive), {
    title: 'My DWebX!!',
    description: 'This dwebx has a manifest!',
    type: 'foo',
    links: {repository: [{href: 'https://github.com/distributedweb/dbrowser-api.git'}]},
    url: `dwebx://${tutil.FAKE_DAT_KEY}`,
    author: 'dwebx://foo.com',
    foobar: true
  })
})


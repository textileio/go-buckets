# go-buckets

[![Made by Textile](https://img.shields.io/badge/made%20by-Textile-informational.svg?style=popout-square)](https://textile.io)
[![Chat on Slack](https://img.shields.io/badge/slack-slack.textile.io-informational.svg?style=popout-square)](https://slack.textile.io)
[![GitHub license](https://img.shields.io/github/license/textileio/go-buckets.svg?style=popout-square)](./LICENSE)
[![GitHub action](https://github.com/textileio/go-buckets/workflows/Test/badge.svg?style=popout-square)](https://github.com/textileio/go-buckets/actions)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=popout-square)](https://github.com/RichardLitt/standard-readme)

> File and dynamic directory storage built on Threads, IPFS, and LibP2P

Join us on our [public Slack channel](https://slack.textile.io/) for news, discussions, and status updates. [Check out our blog](https://medium.com/textileio) for the latest posts and announcements.

## Table of Contents

- [Security](#security)
- [Background](#background)
- [Install](#install)
  - [Buckets Core](#buckets-core)
  - [gRPC Daemon](#grpc-daemon)
  - [gRPC Client](#grpc-client)
  - [Client CLI](#client-cli)
  - [Local-first Library](#local-first-library)
- [Getting Started](#getting-started)
  - [Running Buckets](#running-buckets)
  - [Creating a bucket](#creating-a-bucket)
  - [Creating a private bucket](#creating-a-private-bucket)
  - [Adding files and folders to a bucket](#adding-files-and-folders-to-a-bucket)
  - [Recreating an existing bucket](#recreating-an-existing-bucket)
  - [Creating a bucket from an existing Cid](#creating-a-bucket-from-an-existing-cid)
  - [Exploring bucket contents](#exploring-bucket-contents)
  - [Resetting bucket contents](#resetting-bucket-contents)
  - [Watching a bucket for changes](#watching-a-bucket-for-changes)
  - [Protecting a file with a password](#protecting-a-file-with-a-password)
  - [Sharing bucket files and folders](#sharing-bucket-files-and-folders)
  - [Multi-writer buckets](#multi-writer-buckets)
  - [Deleting a bucket](#deleting-a-bucket)
- [Using the Local Library](#using-the-local-library)
  - [Creating a bucket](#creating-a-bucket-1)
  - [Getting an existing bucket](#getting-an-existing-bucket)
  - [Pushing local files](#pushing-local-files)
  - [Pulling remote changes](#pulling-remote-changes)
- [Developing](#developing)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [License](#license)

## Security

Buckets is still under heavy development and no part of it should be used before a thorough review of the underlying code and an understanding APIs and protocols may change rapidly. There may be coding mistakes, and the underlying protocols may contain design flaws. Please [let us know](mailto:contact@textile.io) immediately if you have discovered a security vulnerability.

Please also read the [security note](https://github.com/ipfs/go-ipfs#security-issues) for [go-ipfs](https://github.com/ipfs/go-ipfs).

## Background

Buckets provide a dynamic wrapper around [UnixFS]([UnixFS](https://github.com/ipfs/go-unixfs)) directories with auto-updating IPNS, static website rendering, access control, and encryption.

## Install

### Buckets Core

```
go get github.com/textileio/go-buckets
```

### gRPC Daemon

-   **Prebuilt package**: See [release assets](https://github.com/textileio/go-buckets/releases/latest)
-   **Docker image**: See the `buckets` tag on [Docker Hub](https://hub.docker.com/r/textile/go-buckets/tags)
-   **Build from the source**: 

```
git clone https://github.com/textileio/go-buckets
cd go-buckets
go get ./cmd/buckd
```

### gRPC Client

```
go get github.com/textileio/go-buckets/api/client
```

### Client CLI

-   **Prebuilt package**: See [release assets](https://github.com/textileio/go-buckets/releases/latest)
-   **Build from the source**: 

```
git clone https://github.com/textileio/go-buckets
cd go-buckets
go get ./cmd/buck
```

### Local-first Library

```
import "github.com/textileio/go-buckets/local"
```

See [Using the Local Library](#using-the-local-library) for more info.

## Getting Started

This section focusses on interacting with buckets using a local daemon and the `buck` CLI. If your application doesn't require a daemon, you can use the [core library](https://github.com/textileio/go-buckets/blob/master/buckets.go) directly.

### Running Buckets

The `buckd` daemon can be run as a server or alongside desktop apps or command-line tools. The easiest way to run `buckd` is by using the provided Docker Compose files. If you're new to Docker and/or Docker Compose, get started [here](https://docs.docker.com/compose/gettingstarted/). You should have `docker-compose` in your `PATH`.

Create an `.env` file and add the following values:  

```
REPO_PATH=~/myrepo
BUCK_LOG_DEBUG=true
```

Copy [this compose file](https://github.com/textileio/go-buckets/blob/master/cmd/buckd/docker-compose.yml) and run it with the following command.

```
docker-compose -f docker-compose.yml up
```

Congrats! Now you have Buckets running locally.

The Docker Compose file starts an IPFS node, which is used to pin bucket files and folders. You could point `buckd` to a different (possibly remote) IPFS node by setting the `BUCK_IPFS_MULTIADDR` variable to a different multiaddress.

### Creating a bucket

First off, take a look at `buck --help`.

```
The Bucket Client.

Manages files and folders in an object storage bucket.

Usage:
  buck [command]

Available Commands:
  add         Add adds a UnixFs DAG locally at path
  cat         Cat bucket objects at path
  decrypt     Decrypt bucket objects at path with password
  destroy     Destroy bucket and all objects
  encrypt     Encrypt file with a password
  help        Help about any command
  init        Initialize a new or existing bucket
  links       Show links to where this bucket can be accessed
  ls          List top-level or nested bucket objects
  pull        Pull bucket object changes
  push        Push bucket object changes
  root        Show bucket root CIDs
  status      Show bucket object changes
  watch       Watch auto-pushes local changes to the remote

Flags:
      --api string   API target (default "127.0.0.1:3006")
  -h, --help         help for buck

Use "buck [command] --help" for more information about a command.
```

A bucket functions a bit like an S3 bucket. It's a virtual filesystem where you can push, pull, list, and cat files. You can share them via web links or render the whole thing as a website or web app. They also function a bit like a Git repository. The point of entry is from a folder on your local machine that is synced to a _remote_.

To get started, initialize a new bucket.

```
mkdir mybucket && cd mybucket
buck init
```

When prompted, give your bucket a name and either opt-in or decline bucket encyption (see [Creating a private bucket](#creating-a-private-bucket) for more about bucket encryption).

You should now see two links for the new bucket on the locally running gateway.

```
> http://127.0.0.1:8000/thread/bafkq3ocmdkrljadlgybtvocytpdw4hbnzygxecxehdp7pfj32lxp34a/buckets/bafzbeifyzfm3kosie25s5qthvvcjrr42ivd7doqhwvu5m4ks7uqv4j5lyi Thread link
> http://127.0.0.1:8000/ipns/bafzbeifyzfm3kosie25s5qthvvcjrr42ivd7doqhwvu5m4ks7uqv4j5lyi IPNS link (propagation can be slow)
> Success! Initialized /path/to/mybucket as a new empty bucket
```

The first URL is the link to the ThreadDB instance. Internally, a collection named `buckets` is created. Each new instance in this collection amounts to a new bucket. However, when you visit this link, you'll notice a custom file browser. This is because the gateway considers the built-in `buckets` collection a special case. You can still view the raw ThreadDB instance by appending `?json=true` to the URL.

The second URL is the bucket's unique IPNS address, which is auto-updated when you add, modify, or delete files.

If you have configured the daemon with DNS settings, you will see a third URL that links to the bucket's WWW address, where it is rendered as a static website / client-side application. See `buckd --help` for more info.

**Important**: If your bucket is private (encrypted), an access token (JWT) will be appended to these links. This token represents your _identity_ across ***all buckets*** and should not be shared without caution.

`buck init` created a configuration folder in `mybucket` called `.textile`. This folder is somewhat like a `.git` folder, as it contains information about the bucket's remote address and local state.

`.textile/config.yml` will look something like,

```
key: bafzbeifyzfm3kosie25s5qthvvcjrr42ivd7doqhwvu5m4ks7uqv4j5lyi
thread: bafkq3ocmdkrljadlgybtvocytpdw4hbnzygxecxehdp7pfj32lxp34a
```

Where `key` is the bucket's unique key, and `thread` is it's ThreadDB ID.

Additionally, `.textile/repo` contains a repository describing the current file structure, which is used to stage changes against the remote.

### Creating a private bucket

Bucket encryption (AES-CTR + AES-512 HMAC) happens entirely within the `buckd`, meaning your data gets encrypted on the way in, and decrypted on the way out. This type of encryption has two goals:

- Obfuscate bucket data / files (the normal goal of encryption)
- Obfuscate directory structure, which amounts to encrypting [IPLD](https://ipld.io/) nodes and their links.

As a result of these goals, we refer to encrypted buckets as _private buckets_. Read more about bucket encryption [here](https://docs.textile.io/buckets/#encryption).

To create a new private bucket, use the `--private` flag with `buck` init or respond `y` when prompted.

In addition to bucket-level encryption, you can also [protect a file with a password](#protecting-a-file-with-a-password).

### Adding files and folders to a bucket

Bucket files and folders are content-addressed by Cids. Check out [the spec](https://github.com/multiformats/cid) if you're unfamiliar with Cids.

`buck` stages new files as additions:

```
echo "hello world" > hello.txt
buck status
> new file:  hello.txt
```

`buck status` is powered by DAG-based diffing. Much like `git`, this allows buck to only push and pull _changes_. Read more about bucket diffing in the [docs](https://docs.textile.io/buckets/#diffing-and-synching), or check out this [in-depth blog post](https://blog.textile.io/buckets-diffing-syncing-archiving/).

Use `push` to sync the change.

```
buck push
+ hello.txt: bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4
> bafybeihm4zrnrsdroazwsvk3i65ooqzdftaugdkjiedr6ocq65u3ap4wni
```

The output shows the Cid of the added file and the bucket's new root Cid.

`push` will sync all types of file changes: _Additions, modifications, and deletions_.

### Recreating an existing bucket

It's often useful to recreate a bucket from the remote. This is somewhat like re-cloning a Git repo. This can be done in a different location on the same machine, or, if `buckd` has a public IP address, from a completely different machine.

Let's recreate the bucket from the previous step in a new directory.

```
mkdir mybucket2 && cd mybucket2
buck init --existing
```

The `--existing` flag allows for interactively selecting an existing bucket to initialize from.

```
? Which exiting bucket do you want to init from?:
  ▸ MyBucket bafzbeifyzfm3kosie25s5qthvvcjrr42ivd7doqhwvu5m4ks7uqv4j5lyi
```

At this point, there's only one bucket to choose from.

```
> Selected bucket MyBucket
+ hello.txt: bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4
+ .textileseed: bafkreifbdzttoqsch5j66hfmcbsic6qvwrikibgzfbg3tn7rc3j63ukk3u
> Your bucket links:
> http://127.0.0.1:8000/thread/bafkq3ocmdkrljadlgybtvocytpdw4hbnzygxecxehdp7pfj32lxp34a/buckets/bafzbeifyzfm3kosie25s5qthvvcjrr42ivd7doqhwvu5m4ks7uqv4j5lyi Thread link
> http://127.0.0.1:8000/ipns/bafzbeifyzfm3kosie25s5qthvvcjrr42ivd7doqhwvu5m4ks7uqv4j5lyi IPNS link (propagation can be slow)
> Success! Initialized /path/to/mybucket2 from an existing bucket
```

Just as before, the output shows the bucket's remote links. However, in this case `init` also pulled down the content.

**Note**: `.textileseed` is used to randomize a bucket's top level Cid and cannot be modified.

The `--existing` flag is really just a helper that sets the `--thread` and `--key` flags, which match the config values we saw earlier. We could have used those flags directly to achieve the same result.

```
buck init --thread bafkq3ocmdkrljadlgybtvocytpdw4hbnzygxecxehdp7pfj32lxp34a --key bafzbeifyzfm3kosie25s5qthvvcjrr42ivd7doqhwvu5m4ks7uqv4j5lyi
```

Lastly, we could have just copied `.textile/config.yml` to a new directory and used `buck pull` to pull down the existing content.

### Creating a bucket from an existing Cid

Sometimes it's useful to create a bucket from a [UnixFS](https://github.com/ipfs/go-unixfs) directory that is already on the IPFS network.

We can simulate this scenario by adding a local folder to IPFS and then using its root Cid to create a bucket with the `--cid` flag. Here's a local directory.

```
.
├── a
│   ├── bar.txt
│   ├── foo.txt
│   └── one
│       ├── baz.txt
│       ├── buz.txt
│       └── two
│           ├── boo.txt
│           └── fuz.txt
├── b
│   ├── foo.txt
│   └── one
│       ├── baz.txt
│       ├── muz.txt
│       ├── three
│       │   └── far.txt
│       └── two
│           └── fuz.txt
└── c
    ├── one.jpg
    └── two.jpg
```

Use the recursvie flag `-r` with `ipfs add`.

```
ipfs add -r .
added QmcDkcMJXZsNnExehsE1Yh6SRWucHa9ruVT82gpL83431W mydir/a/bar.txt
added QmYiUq2U6euWnKag23wFppG12hon4EBDswdoe4MwrKzDBn mydir/a/foo.txt
added QmXrd35ja3kknnmgj5kyDM74jfG8GLJJQGtRpEQpXCLTR3 mydir/a/one/baz.txt
added QmSWJvCzotB3CbdxVu8mBvmLqpSuEQgUoJHTFy1azRfwhT mydir/a/one/buz.txt
added QmT6h1eaBV74Sh75upE7ugFLkBnmyGr3WsQ8w8yx5NjgPV mydir/a/one/two/boo.txt
added QmTdg1b5eWEx4zJtrgvew1inkkZ29fp9mbQ4uHyKurW8Ub mydir/a/one/two/fuz.txt
added QmYiQAk1seXrmuQkpGE83AxJyNZDK1RNSaLyp3Z4r1zsrB mydir/b/foo.txt
added QmXrd35ja3kknnmgj5kyDM74jfG8GLJJQGtRpEQpXCLTR3 mydir/b/one/baz.txt
added QmSWJvCzotB3CbdxVu8mBvmLqpSuEQgUoJHTFy1azRfwhT mydir/b/one/muz.txt
added QmYs12A3CGSTHX4QrsvBe2AvLHEThrapXoTFQpyh8AzpFa mydir/b/one/three/far.txt
added QmTdg1b5eWEx4zJtrgvew1inkkZ29fp9mbQ4uHyKurW8Ub mydir/b/one/two/fuz.txt
added QmaLpwNPwftSQY3w4ZtMfZ8k38D5EgK2bcDuU4UwzREJpi mydir/c/one.jpg
added QmYLiWv2WXQd1m8YyHx4dMoj8B3Kuiuu7pCCoYibkqKyVj mydir/c/two.jpg
added QmT5YXeCfbMuVjanbHjQhECUQSACJLecfmjRBZHvmu5FDU mydir/a/one/two
added QmWh2Wx9Lec4wbEvFbsq4HmYjFmgUFtxNJ8wEVwXjhJ2uk mydir/a/one
added QmSujVHvG8Y3Jv21AbMFNQPphjyqNamh6cvdyXSD1jAtSZ mydir/a
added QmUGSorWDy2JiKYvQuJzEb4TnYDuDNLcdFyR6NhMwnwdvy mydir/b/one/three
added QmWvX7UVexbjXJtxKMyMSgGpPesFQD7teNTqUcDsP2mzW6 mydir/b/one/two
added QmPyMD67EgSZS1WpvgudHkxbA5zgjqmse8srPpFb9sVefT mydir/b/one
added QmQdAtg5NkwkvLtTbka3eci58UGj3m9AehC2sbksGSbjPZ mydir/b
added QmcjtVAF9PQfMKTc57vcvZeBrzww3TLxPcQfUQW7cXXLJL mydir/c
added QmcvkGF2t8Z94UqhdtdFRokGoqypbGyKkzRPVF4owmjVrE mydir
```

After adding the entire directory, we see the root Cid is `QmcvkGF2t8Z94UqhdtdFRokGoqypbGyKkzRPVF4owmjVrE`. Let's create the bucket using this Cid.

```
buck init --cid QmcvkGF2t8Z94UqhdtdFRokGoqypbGyKkzRPVF4owmjVrE
```

The files behind the Cid will be pulled into the new bucket.

```
+ a/bar.txt: QmcDkcMJXZsNnExehsE1Yh6SRWucHa9ruVT82gpL83431W
+ a/foo.txt: QmYiUq2U6euWnKag23wFppG12hon4EBDswdoe4MwrKzDBn
+ a/one/two/fuz.txt: QmTdg1b5eWEx4zJtrgvew1inkkZ29fp9mbQ4uHyKurW8Ub
+ a/one/baz.txt: QmXrd35ja3kknnmgj5kyDM74jfG8GLJJQGtRpEQpXCLTR3
+ c/two.jpg: QmYLiWv2WXQd1m8YyHx4dMoj8B3Kuiuu7pCCoYibkqKyVj
+ b/foo.txt: QmYiQAk1seXrmuQkpGE83AxJyNZDK1RNSaLyp3Z4r1zsrB
+ a/one/buz.txt: QmSWJvCzotB3CbdxVu8mBvmLqpSuEQgUoJHTFy1azRfwhT
+ a/one/two/boo.txt: QmT6h1eaBV74Sh75upE7ugFLkBnmyGr3WsQ8w8yx5NjgPV
+ b/one/muz.txt: QmSWJvCzotB3CbdxVu8mBvmLqpSuEQgUoJHTFy1azRfwhT
+ b/one/three/far.txt: QmYs12A3CGSTHX4QrsvBe2AvLHEThrapXoTFQpyh8AzpFa
+ b/one/baz.txt: QmXrd35ja3kknnmgj5kyDM74jfG8GLJJQGtRpEQpXCLTR3
+ b/one/two/fuz.txt: QmTdg1b5eWEx4zJtrgvew1inkkZ29fp9mbQ4uHyKurW8Ub
+ c/one.jpg: QmaLpwNPwftSQY3w4ZtMfZ8k38D5EgK2bcDuU4UwzREJpi
> Your bucket links:
> http://127.0.0.1:8006/thread/bafk3k3itq2rsybcvhf6wuvumruw3j6cw7ixhrtx4ek45qgvp3e7u2xa/buckets/bafzbeiawo6ghgsqjlorii4wghdl4tzz54x2kiwtcgtaq7b3h5gta2yok2i Thread link
> http://127.0.0.1:8006/ipns/bafzbeiawo6ghgsqjlorii4wghdl4tzz54x2kiwtcgtaq7b3h5gta2yok2i IPNS link (propagation can be slow)
> Success! Initialized /path/to/mybucket3 as a new bootstrapped bucket
```

Currently, UnixFS in `go-ipfs` uses Cid version 0, which is why we see all these old-style Cids started with `Qm`. Of course, you can also use UnixFS directories that use Cid version 1.

Similar to initializing a new bucket from an existing Cid, `buck add` allows you to _add_ and/or _merge in_ an existing UnixFS directory to an _existing bucket_. Like adding new files locally, this works by pulling down the UnixFS content from the IPFS network into the local bucket. Sync the changes with `buck push` as normal.

Pulling an existing UnixFS directory into a new or existing private bucket is also possible. Just opt-in to encryption during initialization as normal. `buckd` will recursively encrypt (without duplicating) the Cid's IPLD file and directory nodes as they are pulled into the new bucket.

### Exploring bucket contents

Use `buck ls [path]` to explore bucket contents. Omitting `[path]` will list the top-level directory.

```
buck ls

  NAME          SIZE     DIR    OBJECTS  CID
  .textileseed  32       false  n/a      bafkreiezexkrnk7yew6glm6sulhur66bbecc2aeaitf7uz4ymmp442lepu
  a             3726     true   3        QmSujVHvG8Y3Jv21AbMFNQPphjyqNamh6cvdyXSD1jAtSZ
  b             3191     true   2        QmQdAtg5NkwkvLtTbka3eci58UGj3m9AehC2sbksGSbjPZ
  c             1537626  true   2        QmcjtVAF9PQfMKTc57vcvZeBrzww3TLxPcQfUQW7cXXLJL
```

Use `[path]` to drill into directories, e.g.,

```
buck ls a

  NAME     SIZE  DIR    OBJECTS  CID
  bar.txt  517   false  n/a      QmcDkcMJXZsNnExehsE1Yh6SRWucHa9ruVT82gpL83431W
  foo.txt  557   false  n/a      QmYiUq2U6euWnKag23wFppG12hon4EBDswdoe4MwrKzDBn
  one      2502  true   3        QmWh2Wx9Lec4wbEvFbsq4HmYjFmgUFtxNJ8wEVwXjhJ2uk
```

`buck cat` functions a lot like `ls`, but cats file contents to stdout.

### Resetting bucket contents

Similar to a `git reset --hard`, you can use `buck pull --hard` to discard local changes that have not been pushed.

Continuing with the bucket above, add, modify, and/or delete some files. `buck status` should show your staged changes.

```
buck status
> modified:  a/bar.txt
> deleted:   a/one/baz.txt
> new file:  b/one/three/car.txt
> deleted:   b/foo.txt
```

Normally, `buck pull` will move your local changes to temporary `.buckpatch` files, apply the remote / upstream changes, then reapply your local changes. However, the `--hard` flag will prune all local changes, resetting the local bucket contents to match the remote exactly.

```
buck pull --hard
+ a/one/baz.txt: QmXrd35ja3kknnmgj5kyDM74jfG8GLJJQGtRpEQpXCLTR3
+ b/foo.txt: QmYiQAk1seXrmuQkpGE83AxJyNZDK1RNSaLyp3Z4r1zsrB
+ a/bar.txt: QmcDkcMJXZsNnExehsE1Yh6SRWucHa9ruVT82gpL83431W
- b/one/three/car.txt
> QmTz6HoC18QQqAEtYhfLc4Fse3LPbSCKV8vouvE88MKjFj
```

Now `buck status` will report `> Everything up-to-date`.

Try `buck pull --help` for more options when pulling the remote.

### Watching a bucket for changes

So far we've seen how a bucket can change locally, but the remote can also change. This could happen for a couple of reasons:

* Changes are pushed from a different bucket copy against the same `buckd`.
* Changes are pushed from a different `buckd` at the ThreadDB layer. This is known as a multi-writer scenario. See [Multi-writer buckets](#multi-writer-buckets) for more.

In either case, it is possible to listen for and apply the remote changes using `buck watch`. This will also watch for local changes and auto-push them to the remote. In this way, multiple copies of the same bucket can be kept in sync.

`watch` will block until it's cancelled with a Ctrl-C.

```
buck watch
> Success! Watching /path/to/mybucket for changes...
```

`watch` will survive network interruptions, reconnecting when possible.

```
> Not connected. Trying to connect...
> Not connected. Trying to connect...
> Not connected. Trying to connect...
> Success! Watching /path/to/mybucket for changes...
```

While `watch` is active, file and folders dropped into the bucket will be automatically pushed.

### Protecting a file with a password

Private buckets handle encryption entirely within `buckd`, but you can use an additional client-side encryption layer with `buck encrypt` to password protect files. This encryption is also AES-CTR + AES-512 HMAC, which means you can efficiently encrypt large file streams. However, unlike bucket-wide encryption in private buckets, client-side encryption is only available for files, not IPLD directory nodes.

Let's create an encrypted version of the `hello.txt` file.

```
buck encrypt hello.txt supersecret > secret.txt
```

`encrypt` writes to stdout. So, here we redirect the output to a new file called `secret.txt`. [scrypt](https://pkg.go.dev/golang.org/x/crypto/scrypt?tab=doc) is used to derive the AES and HMAC keys from a password. This carries the normal tradeoff: _The encryption is only as good as the password_. Also, as with all client-side encryption, you must also store or otherwise remember the password!

`encrypt` only works on local files. You'll have to use `push` to sync the new file to the remote.

```
buck push --yes
+ secret.txt: bafkreiayymufgaut3wrfbzfdxiacxn64mxijj54g2osyk7qnco54iftovi
> bafybeidhffwg5ucwktn7iwyvnkhxpz7b2yrh643bo74cjvsbquzpdgpcd4
```

`decrypt`, on the other hand, works on remote files. So, after pushing `secret.txt`, we can decrypt it (if we can remember the password) and write the plaintext to stdout.

```
buck decrypt secret.txt supersecret
hello world
```

Looks like it worked!

### Sharing bucket files and folders

Bucket contents can be shared with other users using the `buck roles` command. Each file and folder in a bucket maintains a set of public-key based access roles: `None`, `Reader`, `Writer`, and `Admin`. Only the `Admin` role can add and remove files and folders from a shared path. See `buck roles grant --help` for more about each role.

By default, public buckets have two roles located at the top-level path:

```
buck roles ls

  IDENTITY                                                     ROLE
  *                                                            Reader
  bbaareibzpb44ahd7oieqevvlqajidd4jajcvx2vdvti6bpw5wkqolwwerm  Admin

> Found 2 access roles
```

Since access roles are inherited down a bucket path, the single admin role grants the owner full access to all current and future files and folders. The default (`*`) `Read` role indicates that the entire bucket is open to the world. This is merely a reflection of the fact that the underlying UnixFS directory behind public (non-encrypted) buckets are discoverable on the IPFS Network.

Private buckets are not open to the world and are created with only the single admin role. However, we can still grant default (`*`) `Read` access to individual files, folders, or the entire bucket posteriori.

```
buck roles grant "*" myfolder
Use the arrow keys to navigate: ↓ ↑ → ←
? Select a role:
  None
  ▸ Reader
  Writer
  Admin
```

We can now see a new role added to `myfolder`.

```
buck roles ls myfolder

  IDENTITY  ROLE
  *         Reader

> Found 1 access roles
```

Similarly, grant the `None` role to revoke access.

### Multi-writer buckets

Multi-writer buckets leverage the distributed nature of ThreadDB by allowing multiple identities to write to the same bucket hosted by different Libp2p hosts. Since buckets are ThreadDB collection _instances_, this is no different from normal ThreadDB peer collaboration.

### Deleting a bucket

Deleting a bucket is easy—and permanent! `buck destroy` will delete your local bucket as well as the remote, making it unrecoverable with `buck init --existing`.

### Using the Local Library

The `local` library powers both the `buck` CLI. Everything possible in `buck`, from bucket diffing, pushing, pulling, watching, archiving, etc., is available to you in existing projects by importing the Local Library.

```
go get github.com/textileio/go-buckets/local
```

Visit the [GoDoc](https://pkg.go.dev/github.com/textileio/go-buckets/local) for a complete list of methods and more usage descriptions.

#### Creating a bucket

Create a new bucket by constructing a configuration object. Only `Path` is required.

```
// Setup the buckets lib
buckets := local.NewBuckets(cmd.NewClients("api.textile.io:443", false), local.DefaultConfConfig())

// Create a new bucket with config
mybuck, err := buckets.NewBucket(context.Background(), local.Config{
    Path: "path/to/bucket/folder"
})

// Check current status
diff, err := mybuck.DiffLocal() // diff contains staged changes
```

`buckets.NewBucket` will write a local config file and data repo.

See `local.WithName`, `local.WithStrategy`, `local.WithPrivate`, `local.WithCid`, `local.WithInitEvents` for more options when creating buckets.

To create a bucket from an existing remote, use its thread ID and instance ID (bucket `key`) in the config.

#### Getting an existing bucket

`GetLocalBucket` returns the bucket at path.

```
mybuck, err := buckets.GetLocalBucket(context.Background(), "path/to/bucket/folder")
```

#### Pushing local files

`PushLocal` pushes all staged changes to the remote and returns the new local and remote root Cids. These roots will only be different if the bucket is private (the remote is encrypted).

```
newRoots, err := mybuck.PushLocal()
```

See `local.PathOption` for more options when pushing.

#### Pulling remote changes

`PullRemote` pulls all remote changes locally and returns the new root Cids.

```
newRoots, err := mybuck.PullRemote()
```

See `local.PathOption` for more options when pulling.

## Developing

The easiest way to develop against `hubd` or `buckd` is to use the Docker Compose files found in `cmd`. The `-dev` flavored files do not persist repos via Docker Volumes, which may be desirable in some cases.

## Contributing

Pull requests and bug reports are very welcome ❤️

This repository falls under the Textile [Code of Conduct](./CODE_OF_CONDUCT.md).

Feel free to get in touch by:
-   [Opening an issue](https://github.com/textileio/go-buckets/issues/new)
-   Joining the [public Slack channel](https://slack.textile.io/)
-   Sending an email to contact@textile.io

## Changelog

A changelog is published along with each [release](https://github.com/textileio/go-buckets/releases).

## License

[MIT](LICENSE)

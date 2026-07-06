Entrance script is _do-release-docker.sh_. Requires a local docker;
for example, on macOS, Docker for Desktop installed and running.

These scripts are adapted from the Apache Phoenix dev/create-release scripts
(which themselves came from HBase and originally Spark). They produce Apache
Phoenix Adapters release candidates.

For usage, pass '-h':

 $ ./do-release-docker.sh -h

./do-release-docker.sh accepts the following options

  -d [path]    required. working directory. output will be written to "output" in here.
  -f           "force" -- actually publish this release. Unless you specify '-f', it will
               default to dry run mode, which checks and does local builds, but does not upload anything.
  -t [tag]     tag for the phoenix-adapters-rm docker image to use for building (default: "latest").
  -j [path]    path to local JDK installation to use building. By default the script will
               use openjdk8 installed in the docker image.
  -p [project] project to build; defaults to the PROJECT env var (phoenix-adapters).
  -r [repo]    git repo to use for remote git operations. defaults to ASF gitbox for the project.
  -s [step]    runs a single step of the process; valid steps are: tag|publish-dist|publish-release.
               If none specified, runs tag, then publish-dist, and then publish-release.
               'publish-snapshot' is also an allowed, less used, option.
  -h           display usage information
  -x           debug. do less clean up. (env file, gpg forwarding on mac)

For example, use the following command to do a full dry run build:

./do-release-docker.sh -d /tmp/phoenix-adapters-build

To run a build w/o invoking docker (not recommended!), use _do-release.sh_.

Both scripts will query interactively for needed parameters and passphrases.
For explanation of the parameters, execute:
 $ release-build.sh --help

Notes specific to phoenix-adapters:
 * The binary distribution tarball is produced by the "phoenix-ddb-assembly" module rather than
   the conventional "${PROJECT}-assembly" name. This is handled via the ASSEMBLY_MODULE environment
   variable, which do-release-docker.sh and do-release.sh default to "phoenix-ddb-assembly".
 * phoenix-adapters shares the PHOENIX JIRA project. By convention its "Fix Version" values are
   prefixed with "adapters-" (e.g. "adapters-1.0.0"). release-build.sh uses this to generate
   CHANGES.md and RELEASENOTES.md via Apache Yetus releasedocmaker.
 * Release tarballs are staged under https://dist.apache.org/repos/dist/dev/phoenix/ and the RM's
   signing key must be present in https://dist.apache.org/repos/dist/release/phoenix/KEYS .

Before starting the RC build, run a reconciliation of what is in
JIRA with what is in the commit log. Make sure they align and that
anomalies are explained up in JIRA.

See http://hbase.apache.org/book.html#maven.release
(Even though the above documentation is for HBase, we use the same process for Phoenix
and its sub-projects.)

Regardless of where your release build will run (locally, locally in docker, on a remote machine,
etc) you will need a local gpg-agent with access to your secret keys. A quick way to tell gpg
to clear out state and start a gpg-agent is via the following command phrase:

$ gpgconf --kill all && gpg-connect-agent /bye

Before starting an RC build, make sure your local gpg-agent has configs
to properly handle your credentials, especially if you want to avoid
typing the passphrase to your secret key.

e.g. if you are going to run and step away, best to increase the TTL
on caching the unlocked secret via ~/.gnupg/gpg-agent.conf
  # in seconds, e.g. a day
  default-cache-ttl 86400
  max-cache-ttl 86400

In the current version, passphrase entry doesn't work at all, at least for Linux Docker builds.
Increasing the TTL only works if you unlock the key before starting the release script by running
gpg separately before the script.
A better way to handle passphrases without changing the TTLs is to preset the passphrase,
which avoids using pinentry mechanism completely, and will be reset on logout.

# Find the "gpg-preset-passphrase" program. It is not on the PATH by default.
$ find / -name gpg-preset-passphrase
# Make sure you have the "allow-preset-passphrase" line  in your $HOME/.gnupg/gpg-agent.conf
# Restart gpg
$ gpgconf --kill all && gpg-connect-agent /bye
# List your keys with key grip
$ gpg --with-keygrip --list-secret-keys
# Preset the passphrase for your signing key
# </full/path/to/>/gpg-preset-passphrase -P <the passphrase> -c <the keygrip>
# Check that the passphrase is successfully preset. There should be a '1' at the fourth position
# after the keygrip for your key in the output for the signing key
$ gpg-connect-agent 'keyinfo --list' /bye
# Run the release script (see above)
# Restart the gpg agent again to make sure it forgets the preset passphrase
$ gpgconf --kill all && gpg-connect-agent /bye

Note that according to https://www.apache.org/legal/release-policy.html#owned-controlled-hardware
building an Apache release must be done on hardware owned and controlled by the committer.

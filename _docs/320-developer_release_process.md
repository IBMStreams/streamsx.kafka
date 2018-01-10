---
title: "Toolkit Release process"
permalink: /docs/developer/release_process
excerpt: "How to release a new version"
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}

The following steps should be followed to generate a new release:

* In your local workspace, run `git flow release start vX.Y.Z` (assuming git-flow is installed)
  * You should now be in the `release/vX.Y.Z` branch
* Bump `com.ibm.streamsx.kafka/info.xml` version number
* Generate a release by running `gradle release`
* Run a greenthread test against the **release** (not the toolkit in your git repo)
   1. Extract the generated release in `/tmp`
   1. Pick one of the samples and update the `toolkitPath` variable in `build.gradle` to point to `/tmp/com.ibm.streamsx.kafka`
   1. Update the `etc/*.properties` to point to a Kafka cluster
   1. Run `gradle build` to build the sample
   1. Run the sample: `streamtool submitjob output/<sample_name>/<sample_name>.sab`
   1. **DELETE** the toolkit from `/tmp`
* Merge `release/vX.Y.Z` into *both* `master` and `develop`:
   * `gradle clean`
   * `git add com.ibm.streamsx.kafka/info.xml`
   * `git commit -m "Update version to vX.Y.Z"`
   * `git flow release finish vX.Y.Z`
* Push `master` and `develop` to Github repo: `git push`
* Push the tag to the Github repo: `git push --tags`
* Generate a release on Github using the pushed tag
* DONE!
 
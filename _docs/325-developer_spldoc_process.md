---
title: "Toolkit SPLDoc process"
permalink: /docs/developer/spldoc_process
excerpt: "How to generate SPL doc for the gh-pages documentation"
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}

The following steps can be followed to generate new SPLDocs for the toolkit and to add it to the github pages. 

**NOTE:** It is recommended that a new repository be cloned in `/tmp` to isolate doc generation from development work.

**TODO: The following procedure must be verified!**

0. `cd /tmp`
1. `git clone git@github.com:IBMStreams/streamsx.kafka.git`
2. `cd streamsx.kafka`
3. `./gradlew spldoc`
4. `git checkout gh-pages`
5. `pushd doc`
6. `git rm -r '*'`
7. `popd`
8. `cp -r docs/spldoc doc`
9. `git add doc/spldoc`
10. `git commit -m "Update docs for vX.Y.Z`
11. `git push -u origin`

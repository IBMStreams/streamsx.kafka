---
title: "New feature checklist"
permalink: /docs/developer/new_feature_checklist/
excerpt: "New feature checklist"
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}

### New Feature Checklist 

The following are a set of guidelines for adding a new feature

* Add the feature to the code
  * Ensure that the toolkit documentation is updated appropriately
* Create a sample demonstrating how to use the new feature
  * Should be added to `samples` folder
* Add a test case to ensure that the feature gets tested 
  * Add a topology-based test case to the `tests/KafkaTests` project
* Generate the SPLDoc, see [HERE](/streamsx.kafka/docs/developer/spldoc_process)
  * After pushing docs to `gh-pages`, verify that they are visible and correct when navigating to <https://ibmstreams.github.io/streamsx.kafka/>
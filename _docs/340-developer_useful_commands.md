---
title: "Useful commands for the developer"
permalink: /docs/developer/useful_commands/
excerpt: "Some useful console commands for the developer"
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}

### Display topic offsets:

`bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic test1 --broker-list localhost:9091`

##### output
`<topic>:<offset_of_first_message>:<next_offset>`
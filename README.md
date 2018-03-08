# toolkit-theme-2



This project just stared and is incubating.

It is build on and inspired by the [minimal-mistakes](https://github.com/mmistakes/minimal-mistakes) great Jekyll theme and shall provided a unique layout for all Streams toolkit documentation.
Idea is that each toolkit uses this theme on it's gh-pages branch and just adds it's documentation as markdown documents. 
At the end each toolkit will have one place collecting all information on just this one site. Actually there are many places where one can find toolkit specific information. But also if there is information regarding toolkit usage available there is nearly no information regarding development and testing of a toolkit.
This theme shall enable and encourage everyone who is involved in developing and testing toolkits to document as much as possible.

New pages can be created just within the github web by writing markdown files. Navigation topics are just added with two lines in the navigation.yml file.

Lookt here to see it in action [toolkit-theme-2](https://rnostream.github.io/toolkit-theme-2/). This site runs just from this master branch as github-page.
Pull this master to your gh-pages branch and change/add your files in /_docs and your navigation in /_data/navigation.yml.

## How to add content

All content is written Markdown Language and is located in the `_docs` directory. Insert the following header into each single MD file that you create:

```yml
---
title: "Toolkit Development overview"
permalink: /docs/developer/overview/               # make sure this is the same as 'url' in _data/navigation.yml
excerpt: "Contributing to this toolkits development."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"                            #chose one of 'knowledgedocs', 'userdocs' or 'developerdocs' from navigation.yml
---
{% include toc %}
{% include editme %}
```

Make sure that the `permalink` is unique within the MD documents and terminates with a `/` character. 
Add the page with the permalink to the `_data/navigation.yml` file to specify where the content shall be visible in the menu.
The actual filename of the MD files in the `_docs`directory is only important for the `prev` and `next` navigation. 
The alphabetic names of the MD files determine the sequence of the previous-next navigation.

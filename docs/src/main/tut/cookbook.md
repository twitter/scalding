---
layout: docs
title:  "Cookbook"
section: "cookbook"
position: 1
---

{% include_relative cookbook/cookbook.md %}

## Index

{% for x in site.pages %}
  {% if x.section == 'cookbook' %}
- [{{x.title}}]({{site.baseurl}}{{x.url}})
  {% endif %}
{% endfor %}

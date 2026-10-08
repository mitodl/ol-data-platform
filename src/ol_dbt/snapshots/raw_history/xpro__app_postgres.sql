{% snapshot snapshot_raw__xpro__app__postgres__cms_certificatepage %}
{{ raw_history_snapshot('raw__xpro__app__postgres__cms_certificatepage', unique_key='page_ptr_id') }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_externalcoursepage %}
{{ raw_history_snapshot('raw__xpro__app__postgres__cms_externalcoursepage', unique_key='page_ptr_id') }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_externalcoursepage_topics %}
{{ raw_history_snapshot('raw__xpro__app__postgres__cms_externalcoursepage_topics', unique_key='id') }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_courserun %}
{{ raw_history_snapshot('raw__xpro__app__postgres__courses_courserun', unique_key='id') }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_coursetopic %}
{{ raw_history_snapshot('raw__xpro__app__postgres__courses_coursetopic', unique_key='id') }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_product %}
{{ raw_history_snapshot('raw__xpro__app__postgres__ecommerce_product', unique_key='id') }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_productversion %}
{{ raw_history_snapshot('raw__xpro__app__postgres__ecommerce_productversion', unique_key='id') }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__wagtailcore_page %}
{{ raw_history_snapshot('raw__xpro__app__postgres__wagtailcore_page', unique_key='id') }}
{% endsnapshot %}

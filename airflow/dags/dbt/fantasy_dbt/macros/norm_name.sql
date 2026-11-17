{# lowercase, strip periods/apostrophes, strip Jr/Sr/II/III/IV #}
{% macro norm_name(col) -%}
    trim(regexp_replace(regexp_replace(lower({{ col }}), r"[.']", ''), r'\s+(jr|sr|ii|iii|iv)$', ''))
{%- endmacro %}

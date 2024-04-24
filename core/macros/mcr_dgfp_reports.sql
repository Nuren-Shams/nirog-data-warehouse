{% macro mcr_dgfp_health_center_filter(field_name) %}
    AND UPPER(field_name) IN (
        "PALONGKHALI, UKHIA",
        "RATNA PALONG, UKHIYA",
        "JALIA PALONG",
        "BORO MOHESHKHALI UNION HEALTH CENTER",
        "DHALGHATA UNION HEALTH CENTER",
        "HOANOK UNION HEALTH CENTER",
        "KUTUB JOM UNION HEALTH CENTER",
        "MATARBARI UNION HEALTH CENTER",
        "SHAPLAPUR UNION HEALTH CENTER",
        "MOTHER & CHILD WELFARE CENTRE"
    )
{% endmacro %}

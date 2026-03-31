{%- set names = ["ram", "laxman", "hanuman", "Sugrev"] -%}

{% for i in names %}
    {% if i != "ram" %}
        friends of {{ i }}
    {% else %}
        {{ i }} is King
    {% endif %}
{% endfor %}
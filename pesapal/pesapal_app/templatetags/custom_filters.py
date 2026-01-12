from django import template

register = template.Library()

@register.filter(name='get_item')
def get_item(dictionary, key):
    """Get item from dictionary by key"""
    if dictionary and key in dictionary:
        return dictionary.get(key)
    return None
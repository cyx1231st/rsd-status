from __future__ import print_function


NOT_FOUND= "!NOT!FOUND!"

def print_(name, val, level=0):
    print("%s%s: %s" % ("  "*level, name, val))

def print_attr(resource, name, level=0, ignore=False):
    if ignore and not hasattr(resource, name):
        return
    val = getattr(resource, name, NOT_FOUND)
    if callable(val):
        raise RuntimeError("%s is callable of %s!" % (
            name, resource.__class__.__name__))
    name = name.replace("_", " ")
    print_(name, val, level)

def print_attr_mul(resource, names, level=0, ignore=False):
    for name in names:
        print_attr(resource, name, level, ignore)

def print_items(resource, name, level=0, go_items=True):
    names = name.split(".")
    tmp = resource
    for name in names:
        tmp = getattr(tmp, name, NOT_FOUND)
    items = tmp
    if items == NOT_FOUND:
        items = [items]
    else:
        if go_items:
            items = items.items()
    print_(name,
           "; ".join("%s=%s" % (name_, val) for name_, val in items),
           level)

def print_header(header, resource):
    assert hasattr(resource, "identity")

    print("------ %s %s ------"
            % (header, resource.identity))
    print_attr_mul(
        resource,
        ("path", "name", "description"),
        ignore=True)
    print_items(resource, "status")

def print_body(resource, names):
    assert(not ({
        "identity",
        "path",
        "name",
        "description",
        "json"} & set(names)))
    print_attr_mul(resource, names)

def print_body_lists(resource, names):
    pass

def print_body_none(resource, names):
    other_names = []
    print("--")
    for name in names:
        val = getattr(resource, name, NOT_FOUND)
        if callable(val):
            raise RuntimeError("%s is callable of %s!" % (
                name, resource.__class__.__name__))
        if val:
            print_("*"+name, val)
        else:
            other_names.append(name)
    if other_names:
        print_("None", ",".join(other_names))

import functools
import pickle

from .consts import (
    OP_GETATTR,
    OP_GETATTRONCLASS,
    OP_SETATTR,
    OP_DELATTR,
    OP_CALL,
    OP_CALLATTR,
    OP_CALLONCLASS,
    OP_DEL,
    OP_REPR,
    OP_STR,
    OP_HASH,
    OP_PICKLE,
    OP_DIR,
    OP_INIT,
    OP_NEW,
)

DELETED_ATTRS = frozenset(["__array_struct__", "__array_interface__"])

# These attributes are accessed directly on the stub (not directly forwarded)
LOCAL_ATTRS = (
    frozenset(
        [
            "___remote_class_name___",
            "___identifier___",
            "___connection___",
            "___refcount___",
            "___local_overrides___",
            "__class__",
            "__init__",
            "__del__",
            "__delattr__",
            "__dir__",
            "__doc__",
            "__getattr__",
            "__getattribute__",
            "__hash__",
            "__instancecheck__",
            "__init__",
            "__metaclass__",
            "__module__",
            "__new__",
            "__reduce__",
            "__reduce_ex__",
            "__repr__",
            "__setattr__",
            "__slots__",
            "__str__",
            "__weakref__",
            "__dict__",
            "__methods__",
            "__exit__",
        ]
    )
    | DELETED_ATTRS
)

NORMAL_METHOD = 0
STATIC_METHOD = 1
CLASS_METHOD = 2


def fwd_request(stub, request_type, *args, **kwargs):
    connection = object.__getattribute__(stub, "___connection___")
    return connection.stub_request(stub, request_type, *args, **kwargs)


class StubMetaClass(type):
    __slots__ = ()

    def __repr__(self):
        if self.__module__:
            return "<stub class '%s.%s'>" % (self.__module__, self.__name__)
        else:
            return "<stub class '%s'>" % (self.__name__,)

    def __getattr__(cls, name):
        return cls.___class_connection___.stub_request(
            None, OP_GETATTRONCLASS, cls.___class_remote_class_name___, name
        )


def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""
    # Compatibility 2/3. Remove when only 3 support
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)

    return type.__new__(metaclass, "temporary_class", (), {})


class Stub(with_metaclass(StubMetaClass, object)):
    """
    Local reference to a remote object.

    The stub looks and behaves like the remote object but all operations on the stub
    happen on the remote side (server).
    """

    __slots__ = [
        "___remote_class_name___",
        "___identifier___",
        "___connection___",
        "__weakref__",
        "___refcount___",
    ]

    # def __iter__(self):  # FIXME: Keep debugger QUIET!!
    #    raise AttributeError

    def __new__(cls, *args, **kwargs):
        # There are two ways a stub is initialized (Foo for example) is initialized:
        #  - when it is returned from the remote side, in which case we do
        #    Foo(connection, remote_class_name, identifier)
        #  - when it is created locally and needs to actually forward to __init__ on
        #    the remote side. In this case, we want the user to be able to do
        #    Foo(*args, **kwargs) (whatever the usual arguments to __init__ are)
        #
        if len(args) == 3 and id(args[0]) == id(cls.___class_connection___):
            # We don't do anything specific here -- we will handle things in __init__
            # print("In NEW with identifier")
            return super().__new__(cls)
        else:
            # Otherwise, we forward to the remote side. This will create an instance
            # when it comes back from the remote side which we return here. The instance
            # will have been initialized as a proxy on this side (so we would have
            # called __init__ here already but we will forward an OP_INIT call later in
            # case this is actually a base class and __init__ needs to be called later.
            # print(
            #     "In NEW with %s AND %s for cls %s, mro: %s"
            #     % (
            #         ",".join([str(x) for x in args]),
            #         ",".join(["%s:%s" % (str(k), str(v)) for k, v in kwargs.items()]),
            #         str(cls),
            #         str(cls.__mro__),
            #     )
            # )
            v = cls.___class_connection___.stub_request(
                None, OP_NEW, cls.___class_remote_class_name___, *args, **kwargs
            )
            # print("Done with new, return type is %s" % type(v))
            return v

    def __init__(self, *args, **kwargs):
        # This works in coordination with the __new__ function above:
        #   - If we did Foo(connection, remote_class_name, identifier), we actually set
        #     these values here
        #   - If not, we did a remote init which already returned the object fully
        #     baked so we do nothing.

        def proxy_init(connection, remote_class_name, identifier):
            self.___remote_class_name___ = remote_class_name
            self.___identifier___ = identifier
            self.___connection___ = connection
            self.___refcount___ = 1

        if len(args) == 3 and id(args[0]) == id(self.__class__.___class_connection___):
            # This should be connection, remote_class_name and identifier
            # print("Proxy init")
            proxy_init(*args)
        else:
            # print("regular init")
            fwd_request(self, OP_INIT, *args, **kwargs)

    def __del__(self):
        try:
            self.___refcount___ -= 1
            if self.___refcount___ == 0:
                fwd_request(self, OP_DEL, self.___identifier___)
        except Exception:
            # raised in a destructor, most likely on program termination,
            # when the connection might have already been closed.
            # it's safe to ignore all exceptions here
            pass

    def __getattribute__(self, name):
        if name in LOCAL_ATTRS:
            if name == "__doc__":
                return self.__getattr__("__doc__")
            elif name in DELETED_ATTRS:
                raise AttributeError()
            else:
                return object.__getattribute__(self, name)
        elif name == "__call__":  # IronPython issue #10
            return object.__getattribute__(self, "__call__")
        elif name == "__array__":
            return object.__getattribute__(self, "__array__")
        else:
            return object.__getattribute__(self, name)

    def __getattr__(self, name):
        if name in DELETED_ATTRS:
            raise AttributeError()
        return fwd_request(self, OP_GETATTR, name)

    def __delattr__(self, name):
        if name in LOCAL_ATTRS:
            object.__delattr__(self, name)
        else:
            return fwd_request(self, OP_DELATTR, name)

    def __setattr__(self, name, value):
        if name in LOCAL_ATTRS or name in self.___local_overrides___:
            object.__setattr__(self, name, value)
        else:
            fwd_request(self, OP_SETATTR, name, value)

    def __dir__(self):
        return fwd_request(self, OP_DIR)

    def __hash__(self):
        return fwd_request(self, OP_HASH)

    def __repr__(self):
        return fwd_request(self, OP_REPR)

    def __str__(self):
        return fwd_request(self, OP_STR)

    def __exit__(self, exc, typ, tb):
        raise NotImplementedError
        # FIXME
        # return fwd_request(self, OP_CTX_EXIT, exc)  # can't pass type nor traceback

    def __reduce_ex__(self, proto):
        # support for pickling
        return pickle.loads, (fwd_request(self, OP_PICKLE, proto),)


def _make_method(method_type, connection, class_name, name, doc):
    if name == "__call__":

        def __call__(_self, *args, **kwargs):
            return fwd_request(_self, OP_CALL, *args, **kwargs)

        __call__.__doc__ = doc
        return __call__

    def method(_self, *args, **kwargs):
        return fwd_request(_self, OP_CALLATTR, name, *args, **kwargs)

    def static_method(connection, class_name, name, *args, **kwargs):
        return connection.stub_request(
            None, OP_CALLONCLASS, class_name, name, True, *args, **kwargs
        )

    def class_method(connection, class_name, name, cls, *args, **kwargs):
        return connection.stub_request(
            None, OP_CALLONCLASS, class_name, name, False, *args, **kwargs
        )

    if method_type == NORMAL_METHOD:
        m = method
        m.__doc__ = doc
        m.__name__ = name
        return m
    if method_type == STATIC_METHOD:
        m = functools.partial(static_method, connection, class_name, name)
        m.__doc__ = doc
        m.__name__ = name
        m = staticmethod(m)
        return m
    if method_type == CLASS_METHOD:
        m = functools.partial(class_method, connection, class_name, name)
        m.__doc__ = doc
        m.__name__ = name
        m = classmethod(m)
        return m


def create_class(
    connection,
    class_name,
    overriden_methods,
    getattr_overrides,
    setattr_overrides,
    class_methods,
):

    class_dict = {"__slots__": ()}
    for name, doc in class_methods.items():
        method_type = NORMAL_METHOD
        if name.startswith("___s___"):
            name = name[7:]
            method_type = STATIC_METHOD
        elif name.startswith("___c___"):
            name = name[7:]
            method_type = CLASS_METHOD
        if name in overriden_methods:
            if method_type == NORMAL_METHOD:
                class_dict[name] = (
                    lambda override, orig_method: lambda obj, *args, **kwargs: override(
                        obj, functools.partial(orig_method, obj), *args, **kwargs
                    )
                )(
                    overriden_methods[name],
                    _make_method(method_type, connection, class_name, name, doc),
                )
            elif method_type == STATIC_METHOD:
                class_dict[name] = (
                    lambda override, orig_method: lambda *args, **kwargs: override(
                        orig_method, *args, **kwargs
                    )
                )(
                    overriden_methods[name],
                    _make_method(method_type, connection, class_name, name, doc),
                )
            elif method_type == CLASS_METHOD:
                class_dict[name] = (
                    lambda override, orig_method: lambda cls, *args, **kwargs: override(
                        cls, functools.partial(orig_method, cls), *args, **kwargs
                    )
                )(
                    overriden_methods[name],
                    _make_method(method_type, connection, class_name, name, doc),
                )
        elif name not in LOCAL_ATTRS:
            class_dict[name] = _make_method(
                method_type, connection, class_name, name, doc
            )
    # Check for any getattr/setattr overrides
    special_attributes = set(getattr_overrides.keys())
    special_attributes.update(set(setattr_overrides.keys()))
    overriden_attrs = set()
    for attr in special_attributes:
        getter = getattr_overrides.get(attr)
        setter = setattr_overrides.get(attr)
        if getter is not None:
            getter = lambda x, name=attr, inner=getter: inner(
                x, name, lambda y=x, name=name: y.__getattr__(name)
            )
        if setter is not None:
            setter = lambda x, value, name=attr, inner=setter: inner(
                x,
                name,
                lambda val, y=x, name=name: fwd_request(y, OP_SETATTR, name, val),
                value,
            )
            overriden_attrs.add(attr)
        class_dict[attr] = property(getter, setter)
    class_dict["___local_overrides___"] = overriden_attrs
    class_dict["___class_remote_class_name___"] = class_name
    class_dict["___class_connection___"] = connection
    return StubMetaClass(class_name, (Stub,), class_dict)

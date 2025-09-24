import ray


@ray.remote
class ObjectStoreManager:
    """
    A class to manage objects in the Ray object store.
    """

    def __init__(self):
        print("ObjectStoreManager is ready to use the Ray object store.")

    def put_object(self, data):
        """
        Puts an object into the Ray object store.

        This is a "zero-copy" operation for large, serialized objects like
        NumPy arrays, meaning the data is not duplicated on the same machine.

        Returns:
            An ObjectRef, which is a future-like handle to the data.
        """
        print(f"Putting object of type {type(data)} into the object store...")
        return ray.put(data)

    def get_object(self, object_ref):
        """
        Retrieves an object from the Ray object store.

        The `ray.get()` call will block until the object is available locally
        and can be returned.

        Args:
            object_ref: The ObjectRef returned by ray.put().

        Returns:
            The original data object.
        """
        print("Retrieving object from the object store...")
        return ray.get(object_ref)

    def free_object(self, object_ref):
        """
        Frees an object from the Ray object store.

        This hints to Ray that the object is no longer needed and can be
        evicted from memory if required.

        Args:
            object_ref: The ObjectRef to free.
        """
        print("Hinting to Ray that the object can be freed...")
        ray.cancel(object_ref)
        ray.wait(
            [
                object_ref
            ],
            timeout=0.1
        )
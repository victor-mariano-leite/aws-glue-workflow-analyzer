from typing import Any, Callable, Dict, List


def paginate_boto3(
    callable_func: Callable[..., Dict[str, Any]], dict_key: str, **kwargs
) -> List[Dict[str, Any]]:
    """
    Handles pagination for Boto3 API calls using NextToken.

    Parameters
    ----------
    callable_func : Callable[..., Dict[str, Any]]
        The function to call, typically a Boto3 client method that returns paginated results.
    dict_key : str
        The key in the response dictionary that contains the list of items to return.
    kwargs : dict
        The parameters to pass to the callable function.

    Returns
    -------
    List[Dict[str, Any]]
        A list of all items returned by the paginated API call.
    """
    all_items = []
    next_token = None
    while True:
        if next_token:
            kwargs["NextToken"] = next_token
        response = callable_func(**kwargs)
        all_items.extend(response.get(dict_key, []))
        next_token = response.get("NextToken")
        if not next_token:
            break
    return all_items

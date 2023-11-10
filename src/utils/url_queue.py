from collections import deque
from typing import Union, Set


class URLQueue:
    """
    URLQueue is a class representing a queue of URLs with
    First In, First Out (FIFO) behavior.

    Custom logic:
        If count of get is bigger than 500 return None
    Attributes:
        urls (deque): A deque (double-ended queue) to store URLs.
        count (int): Counter for the number of times 'get' has been called.
    Methods:
        add(url): Add a URL to the end of the queue.
        get(): Remove and return the URL from the front of the queue.
                   Returns None if the queue is empty or count is greater than 500.
        is_empty(): Check if the queue is empty.
        add_from_set(urls_set): Add URLs from a set to the end of the queue.
    """

    def __init__(self) -> None:
        """
        Initialize an empty URLQueue.
        """
        self.urls = deque()
        self.used_links = set()
        self.count = 0

    def add(self, url: str) -> None:
        """
        Add a URL to the end of the queue.

        Parameters:
            url (str): The URL to be added to the queue.
        """
        if "mailto" not in url:
            self.urls.append(url)

    def get(self) -> Union[str, None]:
        """
        Remove and return the URL from the front of the queue.
        Returns:
            str: The URL at the front of the queue.
            None: If the queue is empty or count is greater than 500.
        """
        if self.urls and self.count < 500:
            self.count += 1
            ret = self.urls.popleft()
            if ret not in self.used_links:
                self.used_links.add(ret)
                return ret
        else:
            return None

    def is_empty(self) -> bool:
        """
        Check if the queue is empty.

        Returns:
            bool: True if the queue is empty, False otherwise.
        """
        return len(self.urls) == 0

    def add_from_set(self, urls_set: Set[str]) -> None:
        """
        Add URLs from a set to the end of the queue.

        Parameters:
            urls_set (Set[str]): A set of URLs to be added to the queue.
        """
        for url in urls_set:
            self.add(url)

    def __str__(self) -> str:
        return str(self.urls)

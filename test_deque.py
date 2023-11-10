from collections import deque


class URLQueue:
    def __init__(self):
        self.urls = deque()

    def enqueue(self, url):
        self.urls.append(url)

    def dequeue(self):
        if self.urls:
            return self.urls.popleft()
        else:
            return None

    def is_empty(self):
        return len(self.urls) == 0


url_queue = URLQueue()

url_queue.enqueue("https://example1.com")
url_queue.enqueue("https://example2.com")
url_queue.enqueue("https://example3.com")

while not url_queue.is_empty():
    next_url = url_queue.dequeue()
    print(f"Processing URL: {next_url}")

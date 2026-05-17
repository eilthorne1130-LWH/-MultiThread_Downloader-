import argparse
import os
import signal
import sys
import threading
import time
import urllib.request
from urllib.error import URLError, HTTPError
class MultiThreadDownloader:
    """多线程分块下载器"""
    def __init__(self, url, output=None, num_threads=8):
        self.url = url
        self.num_threads = max(1, num_threads)
        self.output = output or self._extract_filename(url)
        self.file_size = 0
        self.downloaded = [0] * self.num_threads
        self.chunk_files = [f"{self.output}.part{i}" for i in range(self.num_threads)]
        self.start_time = 0
        self.paused = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
    # ────────────────── 工具方法 ──────────────────
    @staticmethod
    def _extract_filename(url):
        """从 URL 提取默认文件名"""
        path = urllib.request.urlparse(url).path
        name = path.split("/")[-1]
        return name or "download"
    @staticmethod
    def _format_size(size_bytes):
        """字节数转可读字符串"""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"
    @staticmethod
    def _format_speed(bytes_per_sec):
        """速度转可读字符串"""
        return f"{MultiThreadDownloader._format_size(bytes_per_sec)}/s"
    def _format_time(self, seconds):
        """秒数转可读时间"""
        if seconds < 0 or seconds > 86400 * 365:
            return "--:--:--"
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    # ────────────────── 网络请求 ──────────────────
    def _head(self):
        """发送 HEAD 请求获取文件信息"""
        req = urllib.request.Request(self.url, method="HEAD")
        req.add_header("User-Agent", "Mozilla/5.0")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                accept_ranges = resp.getheader("Accept-Ranges", "")
                content_length = resp.getheader("Content-Length")
                if content_length is None:
                    print("[!] 警告: 服务器未提供 Content-Length，无法确定文件大小")
                    self.file_size = 0
                else:
                    self.file_size = int(content_length)
                # 检查是否支持分片下载
                if "bytes" not in accept_ranges and self.file_size > 0:
                    print("[!] 警告: 服务器不支持 Range 请求，将退化为单线程下载")
                    self.num_threads = 1
                print(f"[*] 文件大小: {self._format_size(self.file_size)}")
                print(f"[*] 线程数:   {self.num_threads}")
                return True
        except (URLError, HTTPError) as e:
            print(f"[!] 连接失败: {e}")
            return False
    def _download_chunk(self, thread_id, start, end):
        """下载指定字节范围的分块"""
        chunk_file = self.chunk_files[thread_id]
        # ── 断点续传: 已下载的字节数 ──
        existing_size = 0
        if os.path.exists(chunk_file):
            existing_size = os.path.getsize(chunk_file)
            self.downloaded[thread_id] = existing_size
            start += existing_size
        if start >= end:
            return  # 已下载完成
        req = urllib.request.Request(self.url)
        req.add_header("User-Agent", "Mozilla/5.0")
        req.add_header("Range", f"bytes={start}-{end - 1}")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                mode = "ab" if existing_size > 0 else "wb"
                with open(chunk_file, mode) as f:
                    while True:
                        if self._stop_event.is_set():
                            return
                        chunk = resp.read(64 * 1024)  # 64KB 缓冲区
                        if not chunk:
                            break
                        f.write(chunk)
                        with self._lock:
                            self.downloaded[thread_id] += len(chunk)
        except (URLError, HTTPError) as e:
            print(f"\n[!] 线程 {thread_id} 下载失败: {e}")
            self._stop_event.set()
    # ────────────────── 进度显示 ──────────────────
    def _progress_thread(self):
        """每秒刷新一次进度条"""
        bar_width = 30
        while not self._stop_event.is_set():
            time.sleep(0.5)
            if self._stop_event.is_set():
                break
            with self._lock:
                total_downloaded = sum(self.downloaded)
            elapsed = time.time() - self.start_time if self.start_time else 1
            speed = total_downloaded / elapsed if elapsed > 0 else 0
            if self.file_size > 0:
                percent = total_downloaded / self.file_size * 100
                filled = int(bar_width * total_downloaded / self.file_size)
                bar = "█" * filled + "░" * (bar_width - filled)
                eta = (self.file_size - total_downloaded) / speed if speed > 0 else 0
                size_info = f"{self._format_size(total_downloaded)}/{self._format_size(self.file_size)}"
            else:
                percent = 0
                bar = "█" * (bar_width // 2) + "░" * (bar_width - bar_width // 2)
                eta = 0
                size_info = self._format_size(total_downloaded)
            status = f"\r  [{bar}] {percent:5.1f}%  {size_info}  {self._format_speed(speed)}  ETA: {self._format_time(eta)}"
            sys.stdout.write(status)
            sys.stdout.flush()
    # ────────────────── 合并文件 ──────────────────
    def _merge(self):
        """合并所有分块文件"""
        print("\n[*] 正在合并分块文件...")
        with open(self.output, "wb") as out:
            for chunk_file in self.chunk_files:
                if os.path.exists(chunk_file):
                    with open(chunk_file, "rb") as f:
                        while True:
                            data = f.read(1024 * 1024)  # 1MB 缓冲区
                            if not data:
                                break
                            out.write(data)
        # 清理临时文件
        for chunk_file in self.chunk_files:
            if os.path.exists(chunk_file):
                os.remove(chunk_file)
        print(f"[✓] 下载完成: {self.output}")
        print(f"    总大小: {self._format_size(os.path.getsize(self.output))}")
    # ────────────────── 主流程 ──────────────────
    def download(self):
        """主下载入口"""
        print(f"=" * 50)
        print(f"  多线程下载器")
        print(f"=" * 50)
        print(f"  URL:  {self.url}")
        print(f"  输出: {self.output}")
        # 1. 检查文件信息
        if not self._head():
            return
        # 2. 计算分块
        if self.file_size == 0:
            # 未知大小，单线程下载
            self.num_threads = 1
            chunks = [(0, -1)]  # -1 代表到文件末尾
        else:
            chunk_size = self.file_size // self.num_threads
            chunks = []
            for i in range(self.num_threads):
                start = i * chunk_size
                end = self.file_size if i == self.num_threads - 1 else start + chunk_size
                chunks.append((start, end))
        # 3. 检查已有进度 (断点续传)
        existing_parts = sum(1 for f in self.chunk_files if os.path.exists(f))
        if existing_parts > 0:
            print(f"[*] 检测到 {existing_parts} 个已有分块，将续传")
        # 4. 注册信号处理 (Ctrl+C 优雅退出)
        def signal_handler(sig, frame):
            print("\n[!] 收到中断信号，正在保存进度...")
            self._stop_event.set()
        original_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal_handler)
        try:
            # 5. 启动进度线程
            self.start_time = time.time()
            progress = threading.Thread(target=self._progress_thread, daemon=True)
            progress.start()
            # 6. 启动下载线程
            threads = []
            for i in range(self.num_threads):
                start, end = chunks[i]
                if end == -1:  # 未知大小
                    end = 10 ** 18  # 一个很大的数
                t = threading.Thread(
                    target=self._download_chunk,
                    args=(i, start, end),
                    daemon=True,
                )
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
        finally:
            signal.signal(signal.SIGINT, original_sigint)
        # 7. 合并文件
        if not self._stop_event.is_set():
            self._stop_event.set()  # 停止进度线程
            time.sleep(0.2)
            sys.stdout.write("\r" + " " * 100 + "\r")  # 清除进度行
            sys.stdout.flush()
            self._merge()
        else:
            print("\n[!] 下载中断，进度已保存")
            print(f"    下次运行 `python {sys.argv[0]} {self.url}` 即可续传")
def main():
    parser = argparse.ArgumentParser(
        description="多线程分块下载器 - 纯标准库实现",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python dl.py https://example.com/file.zip
  python dl.py https://example.com/file.zip -o myfile.zip
  python dl.py https://example.com/file.zip -t 16
        """,
    )
    parser.add_argument("url", help="要下载的文件 URL")
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="输出文件名 (默认从 URL 自动提取)",
    )
    parser.add_argument(
        "-t", "--threads",
        type=int,
        default=8,
        help="下载线程数 (默认: 8)",
    )
    args = parser.parse_args()
    downloader = MultiThreadDownloader(
        url=args.url,
        output=args.output,
        num_threads=args.threads,
    )
    downloader.download()
if __name__ == "__main__":
    main()
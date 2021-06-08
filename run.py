# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import os
import re
import resource
import tarfile

import aiofiles
from tqdm import tqdm

xfa_regex = re.compile(
    rb"<[\r\n \t]*template[\r\n \t]+xmlns=\"http://www.xfa.org/schema/xfa-template/",
    re.IGNORECASE,
)


def is_XFA(content):
    return xfa_regex.search(content) is not None


def is_JS(content):
    clues = (
        b"AFNumber_",
        b"AFSimple_",
        b"AFPercent_",
        b"AFSpecial_",
        b"AFDate_",
        b"/JavaScript",
    )
    return any(clue in content for clue in clues)


def is_tagged(content):
    return b"/MarkInfo" in content and b"/Marked true" in content


xfa = []
js = []
tosource = []
tagged = []


def limit_virtual_memory():
    resource.setrlimit(resource.RLIMIT_AS, (4096 * 1024 * 1024, 4096 * 1024 * 1024))


async def analyze(pdf_path):
    types_path = f"{pdf_path[:-4]}___TYPES___.json"

    try:
        async with aiofiles.open(types_path, "r") as f:
            types = await f.read()
    except FileNotFoundError:
        proc = await asyncio.create_subprocess_exec(
            "qpdf",
            "--stream-data=uncompress",
            pdf_path,
            "-",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            preexec_fn=limit_virtual_memory,
        )

        pdf_content, stderr = await proc.communicate()

        if proc.returncode != 0:
            print(
                "Error while uncompressing {}:\n```\n{}```".format(
                    pdf_path, stderr.decode("ascii", "ignore")
                )
            )
            return

        types = ""
        if is_XFA(pdf_content):
            types += "x"
        if is_JS(pdf_content):
            types += "j"
            if b"toSource" in pdf_content:
                types += "s"
        if is_tagged(pdf_content):
            types += "t"

        async with aiofiles.open(types_path, "w") as f:
            await f.write(types)

    if "x" in types:
        xfa.append(pdf_path)
    if "j" in types:
        js.append(pdf_path)
        if "s" in types:
            tosource.append(pdf_path)
    if "t" in types:
        tagged.append(pdf_path)


async def worker(queue):
    while True:
        path = await queue.get()
        if path is None:
            break
        await analyze(path)


async def producer(directories, queue):
    paths = []
    for directory in directories:
        for root, dirs, files in os.walk(directory):
            for name in files:
                if not name.endswith("pdf"):
                    continue

                paths.append(os.path.join(root, name))

    for path in tqdm(paths):
        await queue.put(path)

    # Poison the receiver, now that the producer is finished.
    for _ in range(queue.maxsize):
        await queue.put(None)


async def main(directories):
    workers_num = min(32, os.cpu_count() + 4)
    queue = asyncio.Queue(workers_num)

    futures = [producer(directories, queue)] + [
        worker(queue) for _ in range(workers_num)
    ]
    await asyncio.wait(futures)

    print(f"Found {len(xfa)} PDFs that use XFA")
    print(f"Found {len(js)} PDFs that use JavaScript")
    print(f"Found {len(set(xfa) & set(js))} PDFs that use XFA and JavaScript")
    print(f"Found {len(tosource)} PDFs that use toSource")
    print(f"Found {len(tagged)} PDFs that have tags")

    for type_name, type_list in (("xfa", xfa), ("js", js), ("tagged", tagged[-42:])):
        with tarfile.open(f"{type_name}.tar.gz", "w:gz") as tar:
            for pdf_path in type_list:
                tar.add(pdf_path)


asyncio.run(main(["crawled", "pdfa"]))

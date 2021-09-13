# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import collections
import json
import os
import re
import resource
import tarfile
import typing

import aiofiles
from tqdm import tqdm

xfa_regex = re.compile(
    rb"<[\r\n \t]*template[\r\n \t]+xmlns=\"http://www.xfa.org/schema/xfa-template/",
    re.IGNORECASE,
)

image_regex = re.compile(
    rb"<[\r\n \t]*image[\r\n \t]*contentType=\"([\w/+]+)\"",
    re.IGNORECASE,
)

used_font_regex = re.compile(
    rb"typeface=\"([^\"]+)\"",
    re.IGNORECASE,
)

embedded_font_regex = re.compile(
    rb"/FontFamily \(([^)]+)\)",
    re.IGNORECASE,
)

rectangle_regex = re.compile(
    rb"<[\r\n \t]*rectangle",
    re.IGNORECASE,
)

purexfa_regex = re.compile(
    rb"<[\n\r \t]*dynamicRender[\n\r \t]*>[\n\r \t]*required[\n\r \t]*<[\n\r \t]*/[\n\r \t]*dynamicRender[\n\r \t]*>",
    re.IGNORECASE,
)

xfa_host_regex = re.compile(
    rb"xfa\.host\.([^\W]+)",
    re.IGNORECASE,
)

exec_menu_item_regex = re.compile(
    rb"app\.execMenuItem\(([^)]+)\)",
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


def is_using_rectangles(content):
    return rectangle_regex.search(content) is not None


def is_encrypted(content):
    return b"/Encrypt " in content


def is_purexfa(content):
    return purexfa_regex.search(content) is not None


xfa = []
js = []
tosource = []
tagged = []
rectangles = []
encrypted = []
purexfa = []
image_types: typing.Counter[str] = collections.Counter()
fonts: typing.Counter[str] = collections.Counter()
xfa_host_funcs: typing.Counter[str] = collections.Counter()
exec_menu_item_items: typing.Counter[str] = collections.Counter()


def limit_virtual_memory():
    resource.setrlimit(resource.RLIMIT_AS, (4096 * 1024 * 1024, 4096 * 1024 * 1024))


async def analyze(pdf_path):
    types_path = f"{pdf_path[:-4]}___TYPES___.json"

    try:
        async with aiofiles.open(types_path, "r") as f:
            types = json.loads(await f.read())
    except FileNotFoundError:
        types = {
            "x": 0,
            "j": 0,
            "s": 0,
            "t": 0,
            "r": 0,
            "i": [],
            "f": [],
            "e": 0,
            "p": 0,
            "xh": [],
            "mi": [],
        }

        async with aiofiles.open(pdf_path, "rb") as f:
            orig_pdf_content = await f.read()

        if is_encrypted(orig_pdf_content):
            types["e"] = 1

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

        used_image_types = set(
            result.decode("ascii") for result in image_regex.findall(pdf_content)
        )
        types["i"] = list(used_image_types)

        used_fonts = set(
            result.decode("ascii", "ignore")
            for result in used_font_regex.findall(pdf_content)
        )
        embedded_fonts = set(
            result.decode("ascii")
            for result in embedded_font_regex.findall(pdf_content)
        )
        types["f"] = list(used_fonts - embedded_fonts)

        used_xfa_host_funcs = set(
            result.decode("ascii") for result in xfa_host_regex.findall(pdf_content)
        )
        types["xh"] = list(used_xfa_host_funcs)

        used_exec_menu_item_items = set(
            result.decode("ascii")
            for result in exec_menu_item_regex.findall(pdf_content)
        )
        types["mi"] = list(used_exec_menu_item_items)

        if is_XFA(pdf_content):
            types["x"] = 1
        if is_JS(pdf_content):
            types["j"] = 1
            if b"toSource" in pdf_content:
                types["s"] = 1
        if is_tagged(pdf_content):
            types["t"] = 1
        if is_using_rectangles(pdf_content):
            types["r"] = 1
        if is_purexfa(pdf_content):
            types["p"] = 1

        async with aiofiles.open(types_path, "w") as f:
            await f.write(json.dumps(types))

    if types["x"]:
        xfa.append(pdf_path)
    if types["j"]:
        js.append(pdf_path)
        if types["s"]:
            tosource.append(pdf_path)
    if types["t"]:
        tagged.append(pdf_path)
    if types["r"]:
        rectangles.append(pdf_path)
    if types["e"]:
        encrypted.append(pdf_path)
    if types["p"]:
        purexfa.append(pdf_path)

    for image_type in types["i"]:
        image_types[image_type] += 1

    for font in types["f"]:
        fonts[font] += 1

    for xfa_host_func in types["xh"]:
        xfa_host_funcs[xfa_host_func] += 1

    for exec_menu_item_item in types["mi"]:
        exec_menu_item_items[exec_menu_item_item] += 1


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

    futures = [asyncio.create_task(producer(directories, queue))] + [
        asyncio.create_task(worker(queue)) for _ in range(workers_num)
    ]
    await asyncio.wait(futures, return_when=asyncio.FIRST_EXCEPTION)
    for future in futures:
        if future.done():
            future.result()

    print(f"Found {len(xfa)} PDFs that use XFA")
    print(f"Found {len(js)} PDFs that use JavaScript")
    print(f"Found {len(set(xfa) & set(js))} PDFs that use XFA and JavaScript")
    print(f"Found {len(tosource)} PDFs that use toSource")
    print(f"Found {len(tagged)} PDFs that have tags")
    print(f"Found {len(rectangles)} PDFs that use rectangles")
    print(f"Found {len(encrypted)} encrypted PDFs")
    print(f"Found {len(purexfa)} pure XFA PDFs")

    print("Most common image types:")
    print(image_types.most_common())

    print("Most common used fonts that are not embedded:")
    print(fonts.most_common())

    print("Most commonly used xfa.host.XXX:")
    print(xfa_host_funcs.most_common())

    print("Most commonly used app.execMenuItem(XXX):")
    print(exec_menu_item_items.most_common())

    for type_name, type_list in (("xfa", xfa), ("js", js), ("tagged", tagged[-42:])):
        with tarfile.open(f"{type_name}.tar.gz", "w:gz") as tar:
            for pdf_path in type_list:
                tar.add(pdf_path)


asyncio.run(main(["crawled", "pdfa"]))

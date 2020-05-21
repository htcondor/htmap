#!/usr/bin/env python3

import subprocess
import sys
import textwrap
import contextlib
import pickle
import traceback
from pathlib import Path

import htcondor
import classad

TRANSFER_PLUGIN_CACHE = "_htmap_transfer_plugin_cache"
USER_URL_TRANSFER_DIR = "_htmap_user_url_transfer"
USER_URL_TRANSFER_WORKING = "_htmap_user_url_transfer_working"


def parse_args():
    """
    The optparse library can't handle the types of arguments that the file
    transfer plugin sends, the argparse library can't be expected to be
    found on machines running EL 6 (Python 2.6), and a plugin should not
    reach outside the standard library, so the plugin must roll its own argument
    parser. The expected input is very rigid, so this isn't too awful.
    """

    # The only argument lists that are acceptable are
    # <this> -classad
    # <this> -infile <input-filename> -outfile <output-filename>
    # <this> -outfile <output-filename> -infile <input-filename>
    if not len(sys.argv) in [2, 5, 6]:
        print_help()
        sys.exit(-1)

    # If -classad, print the capabilities of the plugin and exit early
    if (len(sys.argv) == 2) and (sys.argv[1] == "-classad"):
        print_capabilities()
        sys.exit(0)

    # If -upload, set is_upload to True and remove it from the args list
    is_upload = False
    if "-upload" in sys.argv[1:]:
        is_upload = True
        sys.argv.remove("-upload")

    # -infile and -outfile must be in the first and third position
    if not (
        ("-infile" in sys.argv[1:])
        and ("-outfile" in sys.argv[1:])
        and (sys.argv[1] in ["-infile", "-outfile"])
        and (sys.argv[3] in ["-infile", "-outfile"])
        and (len(sys.argv) == 5)
    ):
        print_help()
        sys.exit(-1)
    infile = None
    outfile = None
    try:
        for i, arg in enumerate(sys.argv):
            if i == 0:
                continue
            elif arg == "-infile":
                infile = sys.argv[i + 1]
            elif arg == "-outfile":
                outfile = sys.argv[i + 1]
    except IndexError:
        print_help()
        sys.exit(-1)

    return {"infile": infile, "outfile": outfile, "upload": is_upload}


def print_help(stream = sys.stderr):
    help_msg = textwrap.dedent(
        """
    Usage: {0} -infile <input-filename> -outfile <output-filename>
           {0} -classad

    Options:
      -classad                    Print a ClassAd containing the capablities of this
                                  file transfer plugin.
      -infile <input-filename>    Input ClassAd file
      -outfile <output-filename>  Output ClassAd file
      -upload
    """
    )
    stream.write(help_msg.format(sys.argv[0]))


def print_capabilities():
    capabilities = {
        "PluginType": "FileTransfer",
        "PluginVersion": "0.1",
        "MultipleFileSupport": True,
        "SupportedMethods": "htmap",
    }
    sys.stdout.write(classad.ClassAd(capabilities).printOld())


def main(args):
    transfers = [
        pickle.load(f.open("rb")) for f in Path(TRANSFER_PLUGIN_CACHE).iterdir()
    ]

    print(f"Found {len(transfers)} URL transfers to process.\n")

    if len(transfers) == 0:
        print("Nothing to do!")

        write_dict_to_file_as_ad(
            {
                "TransferSuccess": True,
                "TransferFileName": "",
                "TransferUrl": "",
            },
            args["outfile"],
        )
        return

    builtin_plugins = htcondor.param["FILETRANSFER_PLUGINS"].split(", ")

    available_methods = {
        plugin: classad.parseOne(
            subprocess.run(
                [plugin, "-classad"], stdout = subprocess.PIPE
            ).stdout.decode("utf-8")
        )["SupportedMethods"].split(",")
        for plugin in reversed(builtin_plugins)
    }

    print("Available plugins and methods (in search order):")
    for k, v in available_methods.items():
        print(f"{k} => {v}")
    print()

    deferred_transfers = []
    for output_file, destination in transfers:
        protocol = determine_protocol(destination)
        plugin = find_first_plugin(available_methods, protocol)
        print(
            f"Will transfer {output_file} to {destination} using protocol {protocol} implemented by plugin {plugin}"
        )
        deferred_transfers.append(
            DeferredTransfer(
                output_file = output_file, destination = destination, plugin = plugin
            )
        )

    # TODO: group transfers by plugin

    working = Path(USER_URL_TRANSFER_WORKING)
    working.mkdir(parents = True, exist_ok = True)
    for transfer in deferred_transfers:
        infile = working / f"{transfer.id}.in"
        outfile = working / f"{transfer.id}.out"

        infile.write_text(
            str(
                classad.ClassAd(
                    {
                        "LocalFileName": str(transfer.output_file),
                        "Url": transfer.destination,
                    }
                )
            )
        )

        cmd = [
            transfer.plugin,
            "-infile",
            str(infile),
            "-outfile",
            str(outfile),
            "-upload",
        ]
        print(f"Invoking {' '.join(cmd)}")
        run_plugin = subprocess.run(
            cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE,
        )

        if run_plugin.returncode != 0:
            print(f"Plugin {transfer.plugin} failed! Its return code was {run_plugin.returncode}")
            print(f"Captured stdout:")
            print(run_plugin.stdout.decode())
            print(f"Captured stderr:")
            print(run_plugin.stderr.decode())

            outfile.rename(Path(args['outfile']))
            sys.exit(-1)

        print(f"Transferred {transfer.output_file} to {transfer.destination} successfully!")

    write_dict_to_file_as_ad(
        {
            "TransferSuccess": True,
            "TransferFileName": "",
            "TransferUrl": "",
        },
        args["outfile"],
    )


class DeferredTransfer:
    def __init__(self, *, output_file, destination, plugin):
        self.output_file = output_file
        self.destination = destination
        self.plugin = plugin

    def __hash__(self):
        return hash((self.output_file, self.destination, self.plugin))

    @property
    def id(self):
        return str(hash(self))


def determine_protocol(url):
    scheme = url.split("://")[0]
    if "+" in scheme:
        handle, provider = scheme.split("+")
    else:
        provider = scheme

    return provider


class NoPluginFound(Exception):
    pass


def find_first_plugin(available_methods, method):
    for plugin, methods in available_methods.items():
        if method in methods:
            return plugin

    raise NoPluginFound(f"No plugin found for {method}. Available methods are {available_methods}.")


def write_dict_to_file_as_ad(dict_, path):
    path = Path(path)
    with path.open(mode = 'w') as f:
        f.write(str(classad.ClassAd(dict_)))


if __name__ == "__main__":
    # Per the design doc, all failures should result in exit code -1.
    # This is true even if we cannot write a ClassAd to the outfile,
    # so we catch all exceptions, try to write to the outfile if we can
    # and always exit -1 on error.
    #
    # Exiting -1 without an outfile thus means one of two things:
    # 1. Couldn't parse arguments.
    # 2. Couldn't open outfile for writing.

    try:
        args = parse_args()
    except Exception:
        sys.exit(-1)

    try:
        try:
            scratch_dir = Path.cwd()
            job_ad = classad.parseOne((scratch_dir / ".job.ad").read_text())
            out, err = scratch_dir / job_ad["Out"], scratch_dir / job_ad["Err"]
            with out.open(mode = "a") as out_file, err.open(mode = "a") as err_file:
                with contextlib.redirect_stdout(out_file), contextlib.redirect_stderr(
                    err_file
                ):
                    print("\n------  TRANSFER PLUGIN OUTPUT  ------\n")
                    print("\n------  TRANSFER PLUGIN ERROR   ------\n", file = sys.stderr)
                    main(args)
        except FileNotFoundError:
            main(args)
    except Exception as e:
        tb = traceback.format_exc().replace('\n', ' ')
        write_dict_to_file_as_ad(
            {
                "TransferSuccess": False,
                "TransferError": f"HTMap transfer plugin failed: {type(e).__name__}: {e} [{tb}]",
            },
            args["outfile"],
        )
        sys.exit(-1)

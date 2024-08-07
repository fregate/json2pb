import os
import pathlib
import shutil
import subprocess
import argparse
import importlib.util
import sys

from google.protobuf import json_format

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def generate_proto_files(proto_dir, output_dir):
    # Check if proto_dir exists
    if not os.path.isdir(proto_dir):
        eprint(f"No {proto_dir} directory.")
        return

    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        # TODO: Store git commit for Proto files
    else:
        # TODO: Check git commit and decide regenerate
        shutil.rmtree(output_dir + "/proto")

    import_dir = pathlib.PurePath(proto_dir).parent
    skip_parts = import_dir.parts.__len__()
    generated_imports = []
    # Iterate .proto files
    for root, _, files in os.walk(proto_dir):
        for file in files:
            if file.endswith(".proto"):
                proto_file_path = os.path.join(root, file)
                command = [
                    'protoc',
                    f'--python_out={output_dir}',
                    f'-I{import_dir}',
                    proto_file_path,
                    "--experimental_allow_proto3_optional"
                ]
                try:
                    subprocess.run(command, check=True)
                    simplified_file = pathlib.PurePath(output_dir) / pathlib.PurePath(*pathlib.PurePath(proto_file_path).parts[skip_parts:])
                    generated_imports.append(
                        simplified_file.with_name(pathlib.PurePath(proto_file_path).stem + "_pb2.py"))
                except subprocess.CalledProcessError as e:
                    eprint(f"Error in {proto_file_path} generation: {e}")

    if output_dir not in sys.path:
        sys.path.append(output_dir)

    return generated_imports

def dynamic_import(module_name, module_path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module

def message_generator(generated_files, pbmsg_type, json):
    if not json:
        raise "No Json specified (or it is empty). Abort"

    module_path = [x for x in generated_files if x.name.startswith(pbmsg_type)]
    if not module_path:
        raise f"Can't find {pbmsg_type} in generated files"

    message_py = dynamic_import(pbmsg_type + "_pb2", module_path[0])

    message_builder = getattr(message_py, pbmsg_type)
    if message_builder:
        message = message_builder()
        try:
            json_format.Parse(json, message)
            return message
        except json_format.ParseError as e:
            raise f"Can't parse Json input to {pbmsg_type} message. Error: {e}"
    else:
        raise f"No {pbmsg_type} in {message_py} module"

def main():
    parser = argparse.ArgumentParser(description='Generate PB data from Json')
    parser.add_argument('-p', '--proto_dir', help='Protofiles directory', required=True)
    parser.add_argument('-o', '--output_dir', help='Output proto generated files', required=True) # TODO: check and use Temp directory if not set
    parser.add_argument('-t', '--type', help='Name of message to create and populate with json', required=True)
    parser.add_argument('-j', '--json', help='Json file (or pass text through pipe operator)', required=False)

    args = parser.parse_args()

    generated = generate_proto_files(args.proto_dir, args.output_dir)
    if not generated:
        eprint(f"No files were generated")
        return

    json: str = ""
    if args.__contains__("json") and args.json:
        json_file = open(args.json)
        json = json_file.read()
    else:
        json = sys.stdin.read()

    message = message_generator(generated, args.type, json)
    sys.stdout.buffer.write(message.SerializeToString())

if __name__ == "__main__":
    main()

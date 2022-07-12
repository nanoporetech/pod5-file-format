def iterate_inputs(input_items, recursive: bool, file_pattern: str):
    pattern = file_pattern
    if recursive:
        pattern = "**/" + file_pattern
    for input_item in input_items:
        if input_item.is_dir():
            for file in input_item.glob(pattern):
                yield file
        else:
            yield input_item

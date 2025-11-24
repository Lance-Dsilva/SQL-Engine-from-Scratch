
from csv_parser import CSVParser
import tempfile
import heapq
import json

def _execute_chunked_external_orderby(self, operations):
    """
    External merge-sort for ORDER BY with chunking.
    """

    parser = CSVParser()

    # Extract ORDER BY operation
    order_op = next(op for op in operations if op['type'] == 'orderby')
    column = order_op['column']
    direction = order_op.get('direction', 'ASC').upper()

    # DESC sorting → fallback to memory mode for simplicity
    if direction == 'DESC':
        all_data = []
        for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
            all_data.extend(chunk)
        self.data = all_data
        self.chunked = False
        return self._execute_normal(operations)

    # Pre-ORDER BY ops (filters/selects)
    simple_ops = []
    post_limit = None

    order_idx = operations.index(order_op)
    for op in operations[:order_idx]:
        if op['type'] in ('filter', 'select'):
            simple_ops.append(op)

    for op in operations[order_idx+1:]:
        if op['type'] == 'limit' and post_limit is None:
            post_limit = op['value']

    # Sorting key
    def sort_key(row):
        val = row.get(column)
        try:
            return float(val)
        except:
            return str(val)

    # Phase 1: Sort each chunk and spill to temp files
    temp_files = []

    for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
        chunk_result = self._process_simple_ops_on_chunk(chunk, simple_ops)
        if not chunk_result:
            continue

        chunk_result.sort(key=sort_key)

        tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        for row in chunk_result:
            tmp.write(json.dumps(row) + "\n")
        tmp.flush()
        tmp.seek(0)
        temp_files.append(tmp)

    if not temp_files:
        return []

    # Phase 2: K-way merge
    heap = []
    for idx, tmp in enumerate(temp_files):
        line = tmp.readline()
        if line:
            row = json.loads(line)
            heapq.heappush(heap, (sort_key(row), idx, row))

    results = []

    while heap:
        _, file_idx, row = heapq.heappop(heap)
        results.append(row)

        if post_limit is not None and len(results) >= post_limit:
            break

        tmp = temp_files[file_idx]
        line = tmp.readline()
        if line:
            next_row = json.loads(line)
            heapq.heappush(heap, (sort_key(next_row), file_idx, next_row))

    # Cleanup temp files
    import os
    for tmp in temp_files:
        os.unlink(tmp.name)

    return results
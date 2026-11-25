"""Import and backup share SQL; backups are independent from the main worker."""
from pathlib import Path
from unittest import mock

import pytest

from powerbi import export_powerbi_dataset as exporter
from scripts.build_powerbi_import import build, m_text
from scripts import run_pipeline_worker as worker


def test_import_sources_match_backup_queries_and_checked_in_files(tmp_path):
    sources = build(tmp_path)
    assert set(exporter.DATASET_QUERIES) == set(exporter.DATASETS)
    for filename, sql in exporter.DATASET_QUERIES.items():
        name = Path(filename).stem
        assert 'Query = ' + m_text(sql.strip()) in sources[name]
        assert 'PostgreSQL.Database(pServer, pDatabase' in sources[name]
        assert (tmp_path / f"{name}.pq").read_text(encoding='utf-8') == (
            Path(exporter.__file__).parent / 'import' / f'{name}.pq').read_text(encoding='utf-8')


def test_export_failure_does_not_replay_completed_warehouse_batch(tmp_path):
    checkpoint = tmp_path / 'checkpoint.json'
    with mock.patch.object(worker, 'build_minio_client'), \
         mock.patch.object(worker, 'list_parquet_files', return_value=['a']), \
         mock.patch.object(worker, 'run_etl_pipeline', return_value={'status': 'SUCCESS'}) as etl, \
         mock.patch.object(exporter, 'export_powerbi_data', side_effect=PermissionError('CSV locked')):
        state = worker.run_cycle({}, checkpoint)
        with pytest.raises(PermissionError, match='CSV locked'):
            exporter.export_powerbi_data()
        worker.run_cycle(state, checkpoint)
        assert state['completed_objects'] == ['a']
        etl.assert_called_once()


@pytest.mark.parametrize('status', ['FAILED', 'LOAD_ERROR'])
def test_failed_etl_status_does_not_advance_checkpoint(tmp_path, status):
    path = tmp_path / 'checkpoint.json'
    with mock.patch.object(worker, 'build_minio_client'), \
         mock.patch.object(worker, 'list_parquet_files', return_value=['a']), \
         mock.patch.object(worker, 'run_etl_pipeline', return_value={'status': status}):
        with pytest.raises(RuntimeError, match='ETL did not complete'):
            worker.run_cycle({}, path)
    assert not path.exists()

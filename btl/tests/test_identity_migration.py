import pytest

pytestmark=pytest.mark.unit


def test_legacy_price_collision_is_visible_and_both_observations_are_recoverable():
    from scripts.plan_identity_migration import plan_migration
    rows=[{'price_id':'legacy','product_id':'tiki_1','price':p,'seller':'S','source':'tiki_crawler','timestamp':ts}
          for p,ts in [(100,'2026-10-01T00:00:00Z'),(200,'2026-10-02T00:00:00Z')]]
    result=plan_migration(rows)
    assert len(result['price_mappings'])==2
    assert len({m['new_id'] for m in result['price_mappings']})==2
    assert result['legacy_collisions']==['legacy']


def test_missing_observation_time_is_reported_not_invented():
    from scripts.plan_identity_migration import plan_migration
    result=plan_migration([{'price_id':'legacy','product_id':'p1','price':100}])
    assert result['unresolved_count']==1
    assert result['price_mappings']==[]


def test_non_object_raw_records_are_reported_without_crashing():
    from scripts.plan_identity_migration import plan_migration
    result=plan_migration([None,42,[],{'payload':[]}])
    assert result['unresolved_count']==4
    assert not result['event_mappings']


def test_missing_source_does_not_get_an_invented_identity():
    from scripts.plan_identity_migration import plan_migration
    result=plan_migration([{'price_id':'legacy','product_id':'p1','price':100,
                            'timestamp':'2026-10-01T00:00:00Z'}])
    assert result['unresolved_count']==1
    assert not result['price_mappings']

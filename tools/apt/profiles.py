"""Named, immutable LIVE MuCQL variants for reproducible ablations.

All variants use identical client admission, safety checks and data operations.
Only the listed performance knobs differ. Controls are deliberately matched.
"""
from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class Profile:
    join_aggregate: int = 1
    phase2_window: int = 1
    prepare_range: int = 1
    lookahead: int = 2
    replenish_threshold: int = 1
    batch_wait_us: int = 0
    # Explicit opt-in. Existing raw and full profiles remain synchronous.
    multi_key_pipeline: int = 0


PROFILES = {
    'raw': Profile(),
    # Matched control: same batching knobs but legacy single-key scheduling.
    'multi_key_control': Profile(join_aggregate=16,phase2_window=16,
                                 batch_wait_us=25),
    # Post real JOIN and PREPARE records for independent keys in a bounded
    # asynchronous Phase-2 window on a dedicated ordering thread.
    'multi_key_async': Profile(join_aggregate=16,phase2_window=16,
                               batch_wait_us=25,multi_key_pipeline=1),
    'join_control': Profile(batch_wait_us=25),
    'join_aggregation': Profile(join_aggregate=16, batch_wait_us=25),
    'phase2_control': Profile(join_aggregate=16, batch_wait_us=25),
    'phase2_pipelined': Profile(join_aggregate=16, phase2_window=16, batch_wait_us=25),
    'prepare_control': Profile(lookahead=64),
    'prepare_compressed': Profile(prepare_range=16, lookahead=64),
    'lookahead': Profile(lookahead=64),
    'replenish_control': Profile(prepare_range=16, lookahead=64, replenish_threshold=1),
    'replenish_proactive': Profile(prepare_range=16, lookahead=64, replenish_threshold=16),
    'full_no_join': Profile(phase2_window=16, prepare_range=16, lookahead=64, replenish_threshold=16, batch_wait_us=25),
    'full_no_phase2': Profile(join_aggregate=16, prepare_range=16, lookahead=64,
                              replenish_threshold=16, batch_wait_us=25),
    'full_no_prepare': Profile(join_aggregate=16, phase2_window=16, lookahead=64,
                               replenish_threshold=16, batch_wait_us=25),
    'full_no_lookahead': Profile(join_aggregate=16, phase2_window=16, prepare_range=16,
                                 lookahead=16, replenish_threshold=16, batch_wait_us=25),
    'full_no_proactive': Profile(join_aggregate=16, phase2_window=16, prepare_range=16,
                                 lookahead=64, replenish_threshold=1, batch_wait_us=25),
    'full': Profile(join_aggregate=16, phase2_window=16, prepare_range=16,
                    lookahead=64, replenish_threshold=16, batch_wait_us=25),
}

# Each pair changes one mechanism; a pair with identical settings is excluded.
# The effect of an optimization depends on what other optimizations are on.
ABLATIONS = [
    ('Async cross-key Phase-2 pipelining', 'multi_key_control', 'multi_key_async'),
    ('JOIN aggregation (raw context)', 'join_control', 'join_aggregation'),
    ('RDMA phase2 pipelining', 'phase2_control', 'phase2_pipelined'),
    ('PREPARE range compression', 'prepare_control', 'prepare_compressed'),
    ('Prepared window depth', 'raw', 'lookahead'),
    ('Proactive replenishment', 'replenish_control', 'replenish_proactive'),
    ('JOIN aggregation (full context)', 'full_no_join', 'full'),
    ('RDMA pipeline (full context)', 'full_no_phase2', 'full'),
    ('PREPARE compression (full context)', 'full_no_prepare', 'full'),
    ('Prepared depth (full context)', 'full_no_lookahead', 'full'),
    ('Proactive trigger (full context)', 'full_no_proactive', 'full'),
    ('Combined optimization vs raw', 'raw', 'full'),
]


def profile(name):
    if name not in PROFILES:
        raise ValueError(f'unknown LIVE profile {name!r}: {", ".join(PROFILES)}')
    value = PROFILES[name]
    if not (1 <= value.prepare_range <= value.lookahead and
            1 <= value.replenish_threshold <= value.lookahead):
        raise ValueError('invalid profile window/range/trigger')
    return asdict(value)

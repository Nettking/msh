"""Federation-wide job efficiency learning.

Every useful execution teaches the federation something about how to execute
the next comparable job more efficiently. Learning never grants authority: it
only ranks candidates that the existing scheduler has already found eligible.
"""

from .contracts import (
    EFFICIENCY_PROTOCOL,
    EFFICIENCY_PROTOCOL_MAJOR,
    EFFICIENCY_PROTOCOL_VERSION,
    EXECUTION_ENVIRONMENT_SCHEMA,
    EXECUTION_MEASUREMENTS_SCHEMA,
    EXECUTION_OBSERVATION_SCHEMA,
    WORKLOAD_CLASS_TIERS,
    WORKLOAD_DESCRIPTOR_SCHEMA,
    ExecutionEnvironment,
    ExecutionMeasurements,
    ExecutionObservation,
    WorkloadDescriptor,
    requirements_fingerprint,
)
from .estimator import (
    AdvertisedHardwareHeuristics,
    BenchmarkEvidenceSource,
    BenchmarkResultEvidence,
    HardwareHeuristicSource,
    PerformanceEstimator,
)
from .federation import (
    EXECUTION_OBSERVATION_EVENT,
    FederationObservationIngest,
    ObservationIngestOutcome,
    enforce_observation_authority,
    observation_event_payload,
)
from .policy import (
    EFFICIENCY_POLICY_SCHEMA,
    EfficiencyPolicy,
    SchedulingObjective,
    expected_completion_millis,
    objective_cost,
)
from .profiles import (
    ANY,
    LEARNED_PROFILE_SCHEMA,
    METRIC_NAMES,
    DecayedStatistic,
    EvidenceTier,
    LearnedProfile,
    PerformanceEstimate,
    ProfileKey,
    observation_profile_keys,
)
from .projection import LEARNING_SNAPSHOT_SCHEMA, learning_snapshot, preferred_nodes
from .ranking import (
    SCHEDULING_DECISION_SCHEMA,
    CandidateEvaluation,
    LearnedProviderRanker,
    SchedulingDecisionRecord,
    default_environment,
    default_workload_descriptor,
    exploration_slot,
)
from .recorder import (
    ExecutionObservationRecorder,
    measurements_from_dispatch,
    observation_id_for,
)
from .runtime import ExecutionEfficiencyRuntime, LearningLifecycleTransport
from .store import (
    EFFICIENCY_SCHEMA_NAME,
    EFFICIENCY_SCHEMA_VERSION,
    SQLiteExecutionLearningStore,
)

__all__ = [
    "ANY",
    "EFFICIENCY_POLICY_SCHEMA",
    "EFFICIENCY_PROTOCOL",
    "EFFICIENCY_PROTOCOL_MAJOR",
    "EFFICIENCY_PROTOCOL_VERSION",
    "EFFICIENCY_SCHEMA_NAME",
    "EFFICIENCY_SCHEMA_VERSION",
    "EXECUTION_ENVIRONMENT_SCHEMA",
    "EXECUTION_MEASUREMENTS_SCHEMA",
    "EXECUTION_OBSERVATION_EVENT",
    "EXECUTION_OBSERVATION_SCHEMA",
    "LEARNED_PROFILE_SCHEMA",
    "LEARNING_SNAPSHOT_SCHEMA",
    "METRIC_NAMES",
    "SCHEDULING_DECISION_SCHEMA",
    "WORKLOAD_CLASS_TIERS",
    "WORKLOAD_DESCRIPTOR_SCHEMA",
    "AdvertisedHardwareHeuristics",
    "BenchmarkEvidenceSource",
    "BenchmarkResultEvidence",
    "CandidateEvaluation",
    "DecayedStatistic",
    "EfficiencyPolicy",
    "EvidenceTier",
    "ExecutionEfficiencyRuntime",
    "ExecutionEnvironment",
    "ExecutionMeasurements",
    "ExecutionObservation",
    "ExecutionObservationRecorder",
    "FederationObservationIngest",
    "HardwareHeuristicSource",
    "LearnedProfile",
    "LearnedProviderRanker",
    "LearningLifecycleTransport",
    "ObservationIngestOutcome",
    "PerformanceEstimate",
    "PerformanceEstimator",
    "ProfileKey",
    "SQLiteExecutionLearningStore",
    "SchedulingDecisionRecord",
    "SchedulingObjective",
    "WorkloadDescriptor",
    "default_environment",
    "default_workload_descriptor",
    "enforce_observation_authority",
    "expected_completion_millis",
    "exploration_slot",
    "learning_snapshot",
    "measurements_from_dispatch",
    "objective_cost",
    "observation_event_payload",
    "observation_id_for",
    "observation_profile_keys",
    "preferred_nodes",
    "requirements_fingerprint",
]

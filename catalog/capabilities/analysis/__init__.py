"""Federation-dispatched JSONL analysis jobs.

The package layers only orchestration on top of the existing capability
primitives (jobs, job store, provider selection, dispatch, lifecycle, artifact
authority). It introduces no second job architecture and no second scheduler.
"""

from .content_store import ContentIdentity, LocalArtifactContentStore
from .contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ANALYSIS_CONTRACT_ID,
    ANALYSIS_CONTRACT_VERSION,
    ANALYSIS_DATA_SLICE_SCHEMA,
    ANALYSIS_ENDPOINT_ID,
    ANALYSIS_PLAN_SCHEMA,
    ANALYSIS_PROTOCOL,
    ANALYSIS_PROTOCOL_VERSION,
    ANALYSIS_RESULT_SCHEMA,
    ORIGIN_AUTOMATIC_DISCOVERY,
    ORIGIN_MANUAL_UPLOAD,
    SLICE_KIND_DATE,
    SLICE_KIND_UPLOAD_BATCH,
    AnalysisPlan,
    AnalysisWorkSlice,
    analysis_capability_requirement,
    analysis_grant_id,
    analysis_provider_attributes,
    build_analysis_job,
)
from .gateway import AnalysisArtifactGateway, AnalysisInputTransport
from .providers import (
    CompositeProviderReportSource,
    FederatedProviderReportSource,
    LocalAnalysisProviderSource,
)
from .scheduler import (
    DECISION_DISPATCHED,
    DECISION_NO_PROVIDER,
    DECISION_TERMINAL,
    FederatedAnalysisScheduler,
    SchedulingOutcome,
    SubmissionOutcome,
)
from .service import AnalysisJobRecord, AnalysisJobRegistry, AnalysisWorkService
from .transport import NodeRoutedDispatchTransport
from .worker import (
    AnalysisExecutionReport,
    AnalysisSliceExecutor,
    FederatedAnalysisHandler,
)

__all__ = [
    "ANALYSIS_CAPABILITY_TYPE",
    "ANALYSIS_CONTRACT_ID",
    "ANALYSIS_CONTRACT_VERSION",
    "ANALYSIS_DATA_SLICE_SCHEMA",
    "ANALYSIS_ENDPOINT_ID",
    "ANALYSIS_PLAN_SCHEMA",
    "ANALYSIS_PROTOCOL",
    "ANALYSIS_PROTOCOL_VERSION",
    "ANALYSIS_RESULT_SCHEMA",
    "DECISION_DISPATCHED",
    "DECISION_NO_PROVIDER",
    "DECISION_TERMINAL",
    "ORIGIN_AUTOMATIC_DISCOVERY",
    "ORIGIN_MANUAL_UPLOAD",
    "SLICE_KIND_DATE",
    "SLICE_KIND_UPLOAD_BATCH",
    "AnalysisArtifactGateway",
    "AnalysisExecutionReport",
    "AnalysisInputTransport",
    "AnalysisJobRecord",
    "AnalysisJobRegistry",
    "AnalysisPlan",
    "AnalysisSliceExecutor",
    "AnalysisWorkService",
    "AnalysisWorkSlice",
    "CompositeProviderReportSource",
    "ContentIdentity",
    "FederatedAnalysisHandler",
    "FederatedAnalysisScheduler",
    "FederatedProviderReportSource",
    "LocalAnalysisProviderSource",
    "LocalArtifactContentStore",
    "NodeRoutedDispatchTransport",
    "SchedulingOutcome",
    "SubmissionOutcome",
    "analysis_capability_requirement",
    "analysis_grant_id",
    "analysis_provider_attributes",
    "build_analysis_job",
]

from __future__ import annotations

import argparse

from goal_agent_lib import AgentConfig, GoalAgent


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Plan and execute desktop goals with task-by-task confirmation.",
    )
    parser.add_argument(
        "goal",
        nargs="*",
        help="Optional goal text. If omitted, the agent asks at startup.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan and log actions without physically executing them.",
    )
    parser.add_argument(
        "--confirm-each-action",
        action="store_true",
        help="Ask before every low-level click/keyboard action, not only every task.",
    )
    args = parser.parse_args()

    config = AgentConfig(
        dry_run=args.dry_run or AgentConfig().dry_run,
        confirm_each_action=args.confirm_each_action or AgentConfig().confirm_each_action,
    )
    agent = GoalAgent(config)

    goal = " ".join(args.goal).strip()
    if goal:
        agent.run_goal(goal)
    else:
        agent.interactive_loop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

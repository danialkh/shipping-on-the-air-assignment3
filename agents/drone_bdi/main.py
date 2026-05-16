#!/usr/bin/env python3
"""
Minimal Belief–Desire–Intention agent prototype for a delivery drone simulator.

Demonstrates autonomy as a sense–deliberate–act loop without requiring JADE/Jason.
Run: python main.py
"""

from __future__ import annotations

import dataclasses
import random
import time
from enum import Enum, auto


class FlightMode(Enum):
    IDLE = auto()
    DELIVER = auto()
    RTB = auto()


@dataclasses.dataclass
class Beliefs:
    battery_pct: float
    wind_mps: float
    distance_to_drop_m: float
    obstacle_detected: bool


@dataclasses.dataclass
class Desires:
    must_deliver: bool
    must_preserve_battery: bool


@dataclasses.dataclass
class Intentions:
    mode: FlightMode


def sense() -> Beliefs:
    return Beliefs(
        battery_pct=max(5.0, min(100.0, 72.0 + random.uniform(-2, 2))),
        wind_mps=random.uniform(0, 8),
        distance_to_drop_m=max(0.0, 320.0 + random.uniform(-40, 40)),
        obstacle_detected=random.random() < 0.15,
    )


def deliberate(b: Beliefs, d: Desires) -> Intentions:
    if b.battery_pct < 18 and b.distance_to_drop_m > 50:
        return Intentions(FlightMode.RTB)
    if b.obstacle_detected and b.wind_mps > 5:
        return Intentions(FlightMode.IDLE)
    if b.distance_to_drop_m < 15:
        return Intentions(FlightMode.IDLE)
    return Intentions(FlightMode.DELIVER)


def act(i: Intentions, b: Beliefs) -> None:
    if i.mode is FlightMode.DELIVER:
        print(f"ACT: advance along corridor; dist={b.distance_to_drop_m:.0f}m wind={b.wind_mps:.1f}m/s")
    elif i.mode is FlightMode.RTB:
        print("ACT: return-to-base — low energy margin")
    else:
        print("ACT: hold / release package / wait for weather gap")


def main():
    desires = Desires(must_deliver=True, must_preserve_battery=True)
    print("Drone BDI agent — Shipping on the Air (prototype)\n")
    for step in range(8):
        print(f"--- cycle {step + 1} ---")
        beliefs = sense()
        intentions = deliberate(beliefs, desires)
        print(
            f"BELIEFS: battery={beliefs.battery_pct:.1f}% wind={beliefs.wind_mps:.1f} "
            f"dist={beliefs.distance_to_drop_m:.0f}m obstacle={beliefs.obstacle_detected}"
        )
        print(f"INTENTION: {intentions.mode.name}")
        act(intentions, beliefs)
        time.sleep(0.4)


if __name__ == "__main__":
    main()

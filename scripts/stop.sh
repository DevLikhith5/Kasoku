#!/usr/bin/env bash
echo "Stopping all Kasoku processes..."
pkill -f "kasoku" 2>/dev/null || true
pkill -f "vite" 2>/dev/null || true
echo "Done."

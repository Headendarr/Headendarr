import React, { useMemo, useState } from "react";

function sanitizeBaseUrl(value) {
  const raw = String(value || "")
    .trim()
    .replace(/\/+$/, "");
  if (!raw) {
    return "";
  }
  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return "";
    }
    if (parsed.pathname && parsed.pathname !== "/") {
      return "";
    }
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return "";
  }
}

function createEntry() {
  return {
    name: "",
    base_url: "",
    token: "",
    verify_tls: true,
    timeout_seconds: 20,
  };
}

export default function PlexServersJsonBuilder() {
  const [entries, setEntries] = useState([createEntry()]);
  const [copied, setCopied] = useState(false);

  const normalised = useMemo(
    () =>
      entries.map((entry) => ({
        name: String(entry.name || "").trim(),
        base_url: sanitizeBaseUrl(entry.base_url),
        token: String(entry.token || "").trim(),
        verify_tls: entry.verify_tls !== false,
        timeout_seconds: Number.isFinite(Number(entry.timeout_seconds))
          ? Math.max(1, Number(entry.timeout_seconds))
          : 20,
      })),
    [entries],
  );

  const invalidIndexes = useMemo(
    () =>
      normalised
        .map((entry, idx) =>
          !entry.name || !entry.base_url || !entry.token ? idx : null,
        )
        .filter((idx) => idx !== null),
    [normalised],
  );

  const outputValue = useMemo(
    () =>
      JSON.stringify(
        normalised.filter(
          (entry) => entry.name && entry.base_url && entry.token,
        ),
      ),
    [normalised],
  );

  const setEntryValue = (index, key, value) => {
    setEntries((current) =>
      current.map((entry, i) =>
        i === index ? { ...entry, [key]: value } : entry,
      ),
    );
  };

  const addEntry = () => {
    setEntries((current) => [...current, createEntry()]);
  };

  const removeEntry = (index) => {
    setEntries((current) => {
      if (current.length <= 1) {
        return current;
      }
      return current.filter((_, i) => i !== index);
    });
  };

  const copyOutput = async () => {
    if (!outputValue) {
      return;
    }
    try {
      await navigator.clipboard.writeText(outputValue);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  };

  return (
    <div className="plex-builder">
      {entries.map((entry, index) => {
        const cleanedBaseUrl = sanitizeBaseUrl(entry.base_url);
        return (
          <div className="plex-builder__row" key={`plex-builder-${index}`}>
            <div className="plex-builder__header">
              <strong>Server {index + 1}</strong>
              <button
                type="button"
                className="button button--sm button--danger"
                onClick={() => removeEntry(index)}
                disabled={entries.length <= 1}
              >
                Remove
              </button>
            </div>
            <label>
              Name
              <input
                type="text"
                value={entry.name}
                onChange={(event) =>
                  setEntryValue(index, "name", event.target.value)
                }
                placeholder="My Plex Server Name"
              />
            </label>
            <label>
              Plex Base URL
              <input
                type="text"
                value={entry.base_url}
                onChange={(event) =>
                  setEntryValue(index, "base_url", event.target.value)
                }
                placeholder="http://192.168.1.10:32400"
              />
            </label>
            <label className="plex-builder__checkbox">
              <input
                type="checkbox"
                checked={entry.verify_tls !== false}
                onChange={(event) =>
                  setEntryValue(index, "verify_tls", event.target.checked)
                }
              />
              Verify TLS certificate
            </label>
            <label>
              Plex Token
              <input
                type="text"
                value={entry.token}
                onChange={(event) =>
                  setEntryValue(index, "token", event.target.value)
                }
                placeholder="Plex token"
              />
            </label>
            <div className="plex-builder__hint">
              Follow{" "}
              <a
                href="https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/"
                target="_blank"
                rel="noopener noreferrer"
              >
                this guide
              </a>{" "}
              to fetch your Plex token.
            </div>
            <label>
              Timeout (seconds)
              <input
                type="number"
                min="1"
                value={entry.timeout_seconds}
                onChange={(event) =>
                  setEntryValue(index, "timeout_seconds", event.target.value)
                }
              />
            </label>
          </div>
        );
      })}

      <div className="plex-builder__actions">
        <button
          type="button"
          className="button button--secondary button--sm"
          onClick={addEntry}
        >
          Add server
        </button>
      </div>

      {invalidIndexes.length > 0 && (
        <div className="alert alert--warning">
          Server entries {invalidIndexes.map((idx) => idx + 1).join(", ")} are
          incomplete and excluded from output.
        </div>
      )}

      <label>
        <strong>Generated PLEX_SERVERS_JSON</strong>
        <textarea readOnly value={outputValue} rows={8} />
      </label>

      <div className="plex-builder__actions">
        <button
          type="button"
          className="button button--primary button--sm"
          onClick={copyOutput}
          disabled={!outputValue}
        >
          {copied ? "Copied" : "Copy JSON"}
        </button>
      </div>
    </div>
  );
}

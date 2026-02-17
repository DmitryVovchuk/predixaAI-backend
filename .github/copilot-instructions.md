# Go Backend Change Instructions (MUST FOLLOW)

These rules apply to **every change** in the Go backend (features, refactors, fixes).  
If any rule cannot be followed, explain why and provide the closest safe alternative.

---

## 0) Definition of “Done”
A change is considered complete only when:
- Code compiles locally
- Unit tests added/updated and passing
- Container(s) rebuilt/restarted for the changed service(s)
- Code is split into readable files (no “god file” growth)
- Best practices applied: validation, errors, logging, security, config hygiene

---

## 1) Containers: Rebuild / Rerun Changed Service
After changing code that affects runtime behavior, you MUST:
1) Identify which container/service is impacted (usually the Go service you edited).
2) Rebuild and restart only the changed container(s), not the whole stack unless required.

### Docker Compose (preferred)
Use one of these patterns (pick the one that matches repo setup):

- Rebuild & restart a single service:
  - `docker compose up -d --build <service-name>`

- If you need a clean rebuild (rare):
  - `docker compose build --no-cache <service-name>`
  - `docker compose up -d <service-name>`

### Verification
- Confirm service is running:
  - `docker compose ps`
- Confirm logs show healthy startup:
  - `docker compose logs -f <service-name>`

Do NOT include secrets in logs.

---

## 2) Unit Tests: Required for Changed/Added Logic
Every change must include tests that cover:
- ✅ new paths
- ✅ modified behavior
- ✅ error paths (at least 1)
- ✅ edge cases (at least 1)

### Rules
- Use Go `testing` package + `testify` only if already used in repo.
- Avoid integration tests unless explicitly requested. Prefer pure unit tests.
- Avoid sleeping/time-based flakiness.
- Tests must be deterministic and runnable in CI.

### Patterns
- Table-driven tests for business logic.
- Mock external dependencies via interfaces.
- If DB involved, abstract repository and test service logic with a fake repo.

### Command
- `go test ./...` (or repo-specific test command if defined)

---

## 3) Code Splitting: Keep Logic in Readable Files
Do NOT add large logic blocks into handlers/controllers.

### Required structure (suggested)
- `internal/<domain>/handler/`  -> HTTP handlers (thin)
- `internal/<domain>/service/`  -> business logic
- `internal/<domain>/repo/`     -> persistence layer (interfaces + implementations)
- `internal/<domain>/models/`   -> DTOs / domain types
- `internal/<domain>/validation/` -> validation helpers
- `internal/<domain>/crypto/` or `pkg/crypto/` -> encryption/decryption utils

### Rules
- Handlers should:
  - parse/validate input
  - call service
  - map service errors to HTTP responses
- Services should:
  - implement behavior
  - return typed errors (not raw strings)
- Repos should:
  - encapsulate DB details
- No circular dependencies.

---

## 4) Best Practices (MANDATORY)

### API input validation
- Validate required fields early.
- Return 400 with clear message on invalid input.
- Never accept impossible states.

### Error handling
- Use sentinel/typed errors for mapping:
  - `ErrNotFound`, `ErrInvalidInput`, `ErrNotConfigured`, etc.
- Wrap errors with context (`fmt.Errorf("...: %w", err)`).
- HTTP layer maps errors:
  - Not found -> 404
  - invalid -> 400
  - not configured -> 400 or 503 depending on repo conventions
  - unexpected -> 500

### Logging
- Structured logging (repo standard: zap/logrus/slog).
- Include requestId/correlationId if available.
- NEVER log passwords, tokens, encrypted blobs, or raw credentials.

### Security
- Secrets only from env/config.
- Encryption keys:
  - validate length/format on startup (fail fast).
- Avoid leaking internal error details in HTTP response.

### Config
- Read env once and store in config struct.
- Provide defaults only when safe.
- If feature depends on env, make behavior explicit.

### Maintain backward compatibility
- If endpoint payload changes, accept old + new fields when feasible.
- Don’t break existing clients unless explicitly approved.

---

## 5) Documentation of Change (Short but Required)
For each change:
- Update or add a short entry in `docs/CHANGELOG_DEV.md` (or repo equivalent):
  - what changed
  - any env vars required
  - how to test locally
  - any migrations needed

---

## 6) PR Hygiene (Optional but Recommended)
- Keep commits focused: one feature/fix per PR.
- Avoid drive-by formatting across unrelated files.

---

## 7) Extra Instructions (added by AI)
### a) Add a “contract test” for request structs
Whenever request/response structs change:
- Add a test that unmarshals JSON examples and asserts expected fields.

### b) Keep handler tests minimal
Prefer testing service logic. Handler tests only for:
- request decoding
- status code mapping
- basic happy path

### c) Always add examples
If adding a new payload field:
- Provide JSON examples in docs or in test fixtures.

---

## Output Requirements for Codex
When implementing a change, Codex must output:
1) List of files changed (paths)
2) The exact docker compose command to rerun changed service
3) The exact `go test` command(s)
4) A brief note of new/updated tests and what they cover
5) Any new env vars or config changes (if any)

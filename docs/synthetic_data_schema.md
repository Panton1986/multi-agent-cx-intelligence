# Stage I Synthetic Data Schema

## Format and shared conventions

- Primary format: UTF-8 JSONL, one JSON object per line; one file per dataset.
- Target counts: 500 customers, 1,000 survey responses, 1,500 support tickets,
  500 call transcripts, 15,000 journey events, 5,000 mobile logs, and 20 product releases.
- Types: `string`, `integer`, `number`, `boolean`, `object`, and `array` are JSON types.
  `timestamp` means a string in ISO 8601 UTC, such as `2026-01-15T09:30:00Z`.
  Use UTC `Z` consistently; fractional seconds are permitted.
- Required fields must be present and non-null. Optional fields may be omitted or null.
  Conditional requirements below override optional status.
- Primary IDs are unique, immutable strings with prefixes `cus_`, `sur_`, `tkt_`,
  `cal_`, `evt_`, `log_`, and `rel_`, respectively.
- Generate the customer registry once; reuse its exact `customer_id` values everywhere.
  Every customer reference must resolve to that registry; do not create orphan records.
- Session and device IDs are strings prefixed `ses_` and `dev_`. A session belongs to
  one customer and one device. Reuse both IDs across related events, logs, and contacts.
  They are correlation keys, not foreign keys to separate datasets.
- Use synthetic identities and reserved example email domains. Free text must contain
  no real personal information, credentials, tokens, or payment card details.

## 1. Customers — 500 records

Output: `customers.jsonl`. Primary identifier: `customer_id`.

| Field | Type | Required | Constraints / meaning |
| --- | --- | --- | --- |
| customer_id | string | Yes | Unique customer identifier |
| full_name | string | Yes | Fictional name |
| email | string | Yes | Unique synthetic address under `example.com` |
| created_at | timestamp | Yes | Account creation time |
| country_code | string | Yes | ISO 3166-1 alpha-2 code |
| preferred_language | string | Yes | Language tag, e.g. `en-AU` |
| customer_segment | string | Yes | `new`, `regular`, or `premium` |
| account_status | string | Yes | `active`, `inactive`, or `closed` |

Small customer example:

```json
{"customer_id":"cus_0001","full_name":"Alex Taylor","email":"alex.taylor@example.com","created_at":"2026-01-15T09:30:00Z","country_code":"AU","preferred_language":"en-AU","customer_segment":"regular","account_status":"active"}
```

## 2. Survey responses — 1,000 records

Output: `survey_responses.jsonl`. Primary identifier: `survey_id`.

| Field | Type | Required | Constraints / meaning |
| --- | --- | --- | --- |
| survey_id | string | Yes | Unique response identifier |
| customer_id | string | Yes | Customer foreign key |
| submitted_at | timestamp | Yes | Response time |
| survey_type | string | Yes | `post_support`, `post_journey`, or `general` |
| csat_score | integer | Yes | 1–5; 1–2 denotes negative CSAT |
| comment | string | No | Synthetic customer feedback |
| ticket_id | string | No | Support ticket foreign key |
| call_id | string | No | Call foreign key |
| event_id | string | No | Journey event foreign key |
| session_id | string | No | Session being evaluated |
| device_id | string | No | Device used in that session |

For `post_support`, require at least one of `ticket_id` or `call_id`.
For `post_journey`, require `event_id`. General surveys need no contact or event link.

## 3. Support tickets — 1,500 records

Output: `support_tickets.jsonl`. Primary identifier: `ticket_id`.

| Field | Type | Required | Constraints / meaning |
| --- | --- | --- | --- |
| ticket_id | string | Yes | Unique ticket identifier |
| customer_id | string | Yes | Customer foreign key |
| opened_at | timestamp | Yes | Ticket creation time |
| closed_at | timestamp | No | Required for `resolved` or `closed` status |
| channel | string | Yes | `email`, `chat`, `phone`, or `in_app` |
| issue_type | string | Yes | `payment_failure`, `app_crash`, `login_problem`, or `other` |
| subject | string | Yes | Short synthetic issue summary |
| description | string | Yes | Synthetic issue details |
| status | string | Yes | `open`, `in_progress`, `escalated`, `resolved`, or `closed` |
| priority | string | Yes | `low`, `medium`, `high`, or `urgent` |
| escalation_level | integer | Yes | 0–3; values above 0 indicate escalation history |
| resolution | string | No | Required for `resolved` or `closed` status |
| session_id | string | No | Session associated with the reported issue |
| device_id | string | No | Associated device |
| release_id | string | No | Product release associated with the issue |

An `escalated` ticket must have `escalation_level` greater than 0.

## 4. Call transcripts — 500 records

Output: `call_transcripts.jsonl`. Primary identifier: `call_id`.

| Field | Type | Required | Constraints / meaning |
| --- | --- | --- | --- |
| call_id | string | Yes | Unique call identifier |
| customer_id | string | Yes | Customer foreign key |
| ticket_id | string | No | Associated support ticket foreign key |
| started_at | timestamp | Yes | Call start |
| ended_at | timestamp | Yes | Call end, strictly after start |
| duration_seconds | integer | Yes | Positive; matches elapsed call time to nearest second |
| issue_type | string | Yes | Same vocabulary as support tickets |
| outcome | string | Yes | `resolved`, `follow_up`, or `escalated` |
| transcript | array of objects | Yes | Nonempty ordered conversation turns |
| session_id | string | No | Related application session |
| device_id | string | No | Related application device |

Each transcript turn contains required `speaker` (`customer` or `agent`), `text` (string),
and `offset_seconds` (number, nonnegative). Offsets are nondecreasing and do not exceed
call duration. An escalated call may link to an escalated ticket; standalone calls are allowed.

## 5. Customer journey events — 15,000 records

Output: `customer_journey_events.jsonl`. Primary identifier: `event_id`.

| Field | Type | Required | Constraints / meaning |
| --- | --- | --- | --- |
| event_id | string | Yes | Unique event identifier |
| customer_id | string | Yes | Customer foreign key |
| occurred_at | timestamp | Yes | Event time |
| session_id | string | Yes | Journey session identifier |
| device_id | string | Yes | Device identifier |
| channel | string | Yes | `web` or `mobile` |
| event_type | string | Yes | See event vocabulary below |
| outcome | string | Yes | `success` or `failure` |
| error_code | string | No | Required for failures; null or absent for success |
| release_id | string | No | Release foreign key; required for mobile events |
| ticket_id | string | No | Ticket linked to a support contact event |
| properties | object | No | Synthetic context, e.g. screen, payment method, retry count |

Event vocabulary: `session_started`, `login_attempt`, `product_view`, `checkout_started`,
`payment_attempt`, `app_crash`, `support_contact`, and `journey_completed`.
`app_crash` always has failure outcome; `journey_completed` always has success outcome.

## 6. Mobile application logs — 5,000 records

Output: `mobile_application_logs.jsonl`. Primary identifier: `log_id`.

| Field | Type | Required | Constraints / meaning |
| --- | --- | --- | --- |
| log_id | string | Yes | Unique log identifier |
| customer_id | string | Yes | Customer foreign key; authenticated or known synthetic user |
| occurred_at | timestamp | Yes | Log time |
| session_id | string | Yes | Application session identifier |
| device_id | string | Yes | Device identifier |
| release_id | string | Yes | Mobile product release foreign key |
| event_id | string | No | Related mobile journey event foreign key |
| platform | string | Yes | `android` or `ios`; matches release platform |
| os_version | string | Yes | Synthetic operating system version |
| log_level | string | Yes | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| log_type | string | Yes | `payment_failure`, `app_crash`, `login_problem`, or `activity` |
| message | string | Yes | Synthetic diagnostic message without sensitive values |
| error_code | string | No | Required for the three failure log types |
| stack_trace | string | No | Synthetic trace for crash diagnostics |

Anonymous users are outside this initial schema; a failed login may involve a known customer.

## 7. Product releases — 20 records

Output: `product_releases.jsonl`. Primary identifier: `release_id`.

| Field | Type | Required | Constraints / meaning |
| --- | --- | --- | --- |
| release_id | string | Yes | Unique release identifier |
| version | string | Yes | Semantic version string; unique within a platform |
| platform | string | Yes | `android`, `ios`, or `web` |
| released_at | timestamp | Yes | Release availability time |
| release_notes | string | Yes | Synthetic changes and fixes |
| known_issues | array of strings | Yes | May be empty; issue categories or summaries |
| rollout_percentage | integer | Yes | 1–100; share of eligible users targeted |

## Relationships and consistency

| Source → target | Requirement | Cardinality |
| --- | --- | --- |
| Surveys, tickets, calls, events, logs → customers | Mandatory `customer_id` | Many records to one customer |
| Surveys → tickets / calls | Conditional for `post_support`; otherwise optional | Many surveys to one contact |
| Surveys → events | Mandatory for `post_journey`; otherwise optional | Many surveys to one event |
| Calls → tickets | Optional `ticket_id` | Many calls to one ticket |
| Events → tickets | Optional `ticket_id` | Many events to one ticket |
| Mobile events / logs → releases | Mandatory `release_id` | Many records to one release |
| Web events / tickets → releases | Optional `release_id` | Many records to one release |
| Logs → events | Optional `event_id` | Many logs to one mobile event |

- Every supplied foreign key must resolve. Linked customer-bearing records must share
  the same `customer_id`, including surveys that reference multiple records.
- Generate customers and releases before dependent records. Cross-links need not exist
  for every record, but all scenario families below must appear in the resulting datasets.
- Customer activity occurs on or after account creation; release-linked activity occurs
  on or after release availability. Ticket closure follows opening; surveys follow the
  referenced event, call end, or ticket opening. Post-resolution surveys follow closure.
- Related session/device references must agree. For optional session-bearing records,
  supplying `session_id` also requires `device_id`. Each device has a consistent platform
  and each session a consistent release. A linked mobile log and event share the same
  release, session, device, and customer.

## Scenario coverage

| Scenario | Correlated evidence |
| --- | --- |
| Payment failure | Failed `payment_attempt`, error code, mobile failure log where applicable, optional ticket/call |
| Application crash | Mobile `app_crash` event, ERROR crash log and release link, optional support escalation |
| Login problem | Failed `login_attempt`, login error log for mobile, optional successful retry |
| Support escalation | Ticket escalation level above 0, optional escalated call and support contact event |
| Negative CSAT | Survey score 1–2 with plausible feedback; link to support/journey when survey type requires it |
| Successful journey | Ordered login, product view, checkout, successful payment, and journey completion within one session |

Include both failures and successful recovery paths. A failure does not always imply a
support contact or negative survey; successful journeys do not require support records.
These are schema requirements only; no synthetic datasets are generated at this stage.

# Combined final bundle — rollback manager + airtight publishing (6 files)

This single zip contains the complete, consistent final state of every file
touched by the rollback-manager and airtight-publishing changes. Unzip once at
your repo root; each file REPLACES yours (saga.py is new).

    unzip -o ~/Downloads/smartcourse-workflow-final.zip -d .

## Files (all self-consistent — saga.py IS included)
  services/temporal-worker/app/workflow/saga.py                    NEW  (RollbackManager)
  services/temporal-worker/app/workflow/publishing_workflow.py     begin_publishing + saga
  services/temporal-worker/app/workflow/enrollment_workflow.py     saga version
  services/temporal-worker/app/worker.py                           begin_publishing registered
  services/temporal-worker/app/activities/publishing_activities.py begin_publishing activity
  services/course-service/app/api/routes/courses.py                status flip removed

## NOT touched (your existing files stay as-is)
  enrollment_activities.py, notifier.py, events/ package, clients/,
  config.py, everything in the other services.

## IMPORTANT — do NOT run `git reset`
Nothing here was committed. All your work is uncommitted changes. A git reset
would DELETE it. There is no commit to roll back; this bundle just adds the one
missing file (saga.py) and finalizes the six workflow files.

## Rebuild
    docker compose up -d --build temporal-worker course-service
    docker compose logs temporal-worker --tail 20
Expect on startup: "Worker connected ... queues: course-publishing, enrollment-processing"
with no ModuleNotFoundError.

## Verify
1. Publish a course -> log: begin_publishing OK -> validate_course OK ->
   process_content DONE -> mark_published OK -> emit_course_published OK.
2. Airtight test: docker compose stop temporal; publish -> HTTP 503; the course
   stays "draft" (not stuck in "publishing"); docker compose start temporal.
3. Rollback test: force mark_published to fail -> log shows
   "Compensating: mark_publish_failed" -> course back to "draft".

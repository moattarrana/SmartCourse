# Airtight publishing — four complete files (no hand-editing)

Unzip at your repo root. Each file REPLACES yours in full:

    unzip -o ~/Downloads/airtight-publishing-complete.zip -d .

Files replaced:
  services/temporal-worker/app/worker.py                         (+ begin_publishing registered)
  services/temporal-worker/app/activities/publishing_activities.py (+ begin_publishing activity)
  services/temporal-worker/app/workflow/publishing_workflow.py    (begin_publishing = step 0)
  services/course-service/app/api/routes/courses.py               (status flip removed from endpoint)

NOT changed (leave as-is): enrollment_activities.py, enrollment_workflow.py,
saga.py, notifier.py, the events/ package, config.py.

## Rebuild
    docker compose up -d --build temporal-worker course-service
    docker compose logs temporal-worker --tail 20

## Verify it works (normal publish)
Publish a course -> worker log shows, in order:
    begin_publishing OK
    validate_course OK
    process_content DONE
    mark_published OK
    emit_course_published OK
    Event delivered to course.events
Course ends in status "published".

## Verify it's airtight (Temporal down)
    docker compose stop temporal
    # publish a course through the gateway -> you get HTTP 503
    # GET the course -> status is STILL "draft" (not stuck in "publishing")
    docker compose start temporal
Before this change the course would have been stuck in "publishing" forever.

## Verify rollback (a step fails)
Force mark_published to fail (e.g. block course-service's /internal briefly) ->
worker log shows:
    Compensating: mark_publish_failed
Course returns to "draft".

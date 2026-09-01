# Apply guide — split producers into events/, move notifier into notifications/

This zip adds two new packages. Two existing files need a tiny hand-edit, and two
old flat files get deleted. Unzip at your repo root:

    unzip -o ~/Downloads/temporal-worker-producers-split.zip -d .

## 1. New files added by the zip (nothing to do — they just appear)

    services/temporal-worker/app/events/__init__.py
    services/temporal-worker/app/events/kafka.py              # shared Kafka transport
    services/temporal-worker/app/events/course_events.py      # send_course_published()
    services/temporal-worker/app/events/enrollment_events.py  # send_student_enrolled()
    services/temporal-worker/app/notifications/__init__.py
    services/temporal-worker/app/notifications/notifier.py    # moved out of app/notifier.py

## 2. Edit  app/activities/publishing_activities.py   (2 lines)

Change the import:
    -  from app import event_producer
    +  from app.events import course_events

Change the call inside emit_course_published:
    -  event_producer.publish_course_published(course_id)
    +  course_events.send_course_published(course_id)

Leave the CourseServiceClient import line exactly as it is.

## 3. Edit  app/activities/enrollment_activities.py   (2 edits)

Change the imports (Kafka producer from events/, notifier from notifications/):
    -  from app import event_producer, notifier
    +  from app import notifier                       # if your notifier import was already separate, see note
    +  from app.notifications import notifier
    +  from app.events import enrollment_events

  -> Net result: the file should import notifier from app.notifications, and
     enrollment_events from app.events. It must NOT import app.event_producer.
     Final two lines:
         from app.notifications import notifier
         from app.events import enrollment_events

Change the emit call:
    -  event_producer.publish_student_enrolled(enrollment_id, student_id, course_id)
    +  enrollment_events.send_student_enrolled(enrollment_id, student_id, course_id)

DO NOT touch the notifier.enqueue_welcome_notification(...) line — only its import
path changed (app -> app.notifications). The Celery/enqueue step stays as-is.

Leave the EnrollmentServiceClient import line exactly as it is.

## 4. Delete the two old flat files

    rm services/temporal-worker/app/event_producer.py
    rm services/temporal-worker/app/notifier.py

## 5. Verify nothing points at the old modules (should print NOTHING)

    cd services/temporal-worker
    grep -rn "event_producer\|publish_course_published\|publish_student_enrolled" app/
    grep -rn "from app import notifier\|app.notifier" app/

## 6. Rebuild and test

    docker compose up -d --build temporal-worker
    docker compose logs temporal-worker --tail 15

Publish a course -> logs should still show:
    emit_course_published START / OK
    Event delivered to course.events
Enroll a student -> logs should still show:
    emit_student_enrolled START / OK
    Event delivered to enrollment.events
    enqueue_welcome_notification START / OK
(The activity NAMES did not change; only the producer functions they call moved
and got renamed.)

## Left flat on purpose (with reasons for your mentor)
- app/worker.py  : the entrypoint (`python -m app.worker`), referenced by the
  Dockerfile CMD. Entrypoints conventionally live at the package root.
- app/config.py  : the single central settings module, imported as `app.config`
  by nearly every file. It is one module, not a group needing a folder; moving it
  rewrites every import for no structural gain.

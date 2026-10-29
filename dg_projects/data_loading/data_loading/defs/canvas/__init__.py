"""Canvas course exports, moved here from the canvas code location (issue #2260).

The sheet sensor keeps one ``canvas_course_ids`` partition per course listed in
the course ID sheet, and the schedule re-exports every partition every six hours.
Telling MIT Learn about an export is delivery's job (delivery.assets.canvas).
"""

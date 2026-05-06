from src.services.triage import ProcessingLane, TranscriptTriage


triage = TranscriptTriage()


def test_rebook_confirmed_goes_hot(sample_transcripts):
    result = triage.triage(sample_transcripts["rebook_confirmed"]["transcript"])

    assert result.lane == ProcessingLane.HOT
    assert result.detected_stage == "rebook_confirmed"


def test_demo_booked_goes_hot(sample_transcripts):
    result = triage.triage(sample_transcripts["demo_booked"]["transcript"])

    assert result.lane == ProcessingLane.HOT
    assert result.detected_stage == "demo_booked"


def test_escalation_needed_goes_hot(sample_transcripts):
    result = triage.triage(sample_transcripts["escalation_needed"]["transcript"])

    assert result.lane == ProcessingLane.HOT
    assert result.detected_stage == "escalation_needed"


def test_not_interested_goes_cold(sample_transcripts):
    result = triage.triage(sample_transcripts["not_interested"]["transcript"])

    assert result.lane == ProcessingLane.COLD
    assert result.detected_stage == "not_interested"


def test_already_purchased_goes_cold(sample_transcripts):
    result = triage.triage(sample_transcripts["already_purchased"]["transcript"])

    assert result.lane == ProcessingLane.COLD
    assert result.detected_stage == "already_done"


def test_callback_requested_goes_cold(sample_transcripts):
    result = triage.triage(sample_transcripts["callback_requested"]["transcript"])

    assert result.lane == ProcessingLane.COLD
    assert result.detected_stage == "callback_requested"


def test_considering_goes_cold(sample_transcripts):
    result = triage.triage(sample_transcripts["hinglish_ambiguous"]["transcript"])

    assert result.lane == ProcessingLane.COLD
    assert result.detected_stage == "considering"


def test_short_wrong_number_goes_skip(sample_transcripts):
    result = triage.triage(sample_transcripts["short_call_hangup"]["transcript"])

    assert result.lane == ProcessingLane.SKIP
    assert result.detected_stage in {"short_call", "wrong_number"}
    assert result.reason == "transcript_has_fewer_than_4_turns"

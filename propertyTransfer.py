from flask import request
from flask_restful import Resource
from data_pm import connect
import json
import boto3
import os
from datetime import date, datetime, timedelta
from data_pm import connect, uploadImage, deleteImage, deleteFolder, s3, processImage


def send_ownership_announcement(
    db, sender_id, receiver_id, transfer_id, property_id, percent, mode="REQUEST"
):
    """
    Send announcement for ownership transfer
    mode: "REQUEST" when transfer is created, "APPROVED" when accepted, "REJECTED" when declined
    """
    print("\n" + "=" * 80)
    print(f"ANNOUNCEMENT DEBUG - Starting send_ownership_announcement")
    print(f" Mode: {mode}")
    print(f"Sender ID: {sender_id} (type: {type(sender_id)})")
    print(f"Receiver ID: {receiver_id} (type: {type(receiver_id)})")
    print(f"Transfer ID: {transfer_id}")
    print(f"Property ID: {property_id}")
    print(f"Percent: {percent}")
    print("=" * 80)

    current_datetime = datetime.now().strftime("%m-%d-%Y %H:%M:%S")

    if mode == "REQUEST":
        title = "New Ownership Transfer Request"
        message = f"You have received a request to receive {percent}% ownership of property {property_id}. Transfer ID: {transfer_id}"
    elif mode == "APPROVED":
        title = "Ownership Transfer Approved"
        message = f"Your ownership transfer of {percent}% for property {property_id} has been approved. Transfer ID: {transfer_id}"
    elif mode == "REJECTED":
        title = "Ownership Transfer Declined"
        message = f"Your ownership transfer of {percent}% for property {property_id} has been declined. Transfer ID: {transfer_id}"
    else:
        title = "Ownership Transfer Update"
        message = f"Status update for ownership transfer {transfer_id}"

    announcement_data = {
        "announcement_title": title,
        "announcement_msg": message,
        "announcement_sender": sender_id,
        "announcement_receiver": receiver_id,
        "announcement_mode": "OWNERSHIP_TRANSFER",
        "announcement_properties": json.dumps({receiver_id: [property_id]}),
        "announcement_date": current_datetime,
        "App": "1",
    }

    print(f"\n ANNOUNCEMENT DATA TO INSERT:")
    for key, value in announcement_data.items():
        print(f"   {key}: {value}")

    try:
        print(f"\n Attempting to insert announcement into database...")
        result = db.insert("announcements", announcement_data)
        print(f" ANNOUNCEMENT SENT SUCCESSFULLY!")
        print(f"   Insert Result: {result}")
        print(
            f"   Affected Rows: {result.get('affected_rows', 'N/A') if isinstance(result, dict) else 'N/A'}"
        )
        print("=" * 80 + "\n")
        return result
    except Exception as e:
        print(f" ERROR SENDING ANNOUNCEMENT!")
        print(f"   Error Type: {type(e).__name__}")
        print(f"   Error Message: {str(e)}")
        import traceback

        print(f"   Traceback:\n{traceback.format_exc()}")
        print("=" * 80 + "\n")
        return None


def updateImages(imageFiles, property_uid):
    content = []
    bucket = os.getenv("BUCKET_NAME")

    for filename in imageFiles:
        if type(imageFiles[filename]) == str:
            key = imageFiles[filename].split(f"/{bucket}/")[1]
            data = s3.get_object(Bucket=bucket, Key=key)
            imageFiles[filename] = data["Body"]
            content.append(data["ContentType"])
        else:
            content.append("")

    s3Resource = boto3.resource("s3")
    bucket = s3Resource.Bucket(f"{bucket}")
    bucket.objects.filter(Prefix=f"properties/{property_uid}/").delete()
    images = []
    for i in range(len(imageFiles.keys())):
        filename = f"img_{i-1}"
        if i == 0:
            filename = "img_cover"
        key = f"properties/{property_uid}/{filename}"
        image = uploadImage(imageFiles[filename], key, content[i])
        images.append(image)
    return images


class OwnershipTransfer(Resource):
    def get(self, property_id=None):
        print("In OwnershipTransfer GET")
        response = {}

        if not property_id:
            response["message"] = "Please provide property_id"
            return response

        print("Property ID: ", property_id)

        with connect() as db:
            # Get all ownership transfer info for a specific property
            ownershipQuery = db.execute(
                """
                SELECT 
                    ot.transfer_id,
                    ot.property_id,
                    ot.ownerId,
                    ot.to_owner_id,
                    ot.current_percent,
                    ot.proposed_percent,
                    ot.transfer_status,
                    opi.owner_email,
                    opi.owner_first_name,
                    opi.owner_last_name,
                    to_owner.owner_email AS to_owner_email,
                    to_owner.owner_first_name AS to_owner_first_name,
                    to_owner.owner_last_name AS to_owner_last_name,
                    po.po_owner_percent,
                    po.po_start_date,
                    po.po_end_date
                FROM space_dev.ownershipTransfer AS ot
                LEFT JOIN space_dev.ownerProfileInfo AS opi ON ot.ownerId = opi.owner_uid
                LEFT JOIN space_dev.ownerProfileInfo AS to_owner ON ot.to_owner_id = to_owner.owner_uid
                LEFT JOIN space_dev.property_owner AS po ON ot.property_id = po.property_id AND ot.ownerId = po.property_owner_id
                WHERE ot.property_id = \'"""
                + property_id
                + """\'                AND NOT (
                    ot.transfer_status = 'PENDING'
                    AND EXISTS (
                        SELECT 1 FROM space_dev.ownershipTransfer AS approved
                        WHERE approved.property_id = ot.property_id
                        AND approved.ownerId = ot.ownerId
                        AND approved.to_owner_id = ot.to_owner_id
                        AND approved.proposed_percent = ot.proposed_percent
                        AND approved.transfer_status = 'APPROVED'
                        AND approved.transfer_id > ot.transfer_id
                    )
                )
                ORDER BY ot.transfer_id DESC                """
            )

            response["result"] = ownershipQuery.get("result", [])
            response["count"] = len(response["result"])
            response["message"] = "Ownership transfer data retrieved successfully"
            return response

    def post(self):
        print("In OwnershipTransfer POST")
        response = {}

        print("=== DEBUG: request.form type:", type(request.form))
        print("=== DEBUG: request.form keys:", list(request.form.keys()))
        print("=== DEBUG: request.form dict:", dict(request.form))
        print("=== DEBUG: request.content_type:", request.content_type)

        payload = request.form
        print("OwnershipTransfer POST Payload: ", payload)

        property_id = payload.get("property_id")
        from_owner_email = payload.get("from_owner_email")
        to_owner_email = payload.get("to_owner_email")
        proposed_percent = payload.get("proposed_percent")

        print(f"property_id: {property_id}")
        print(f"from_owner_email: {from_owner_email}")
        print(f"to_owner_email: {to_owner_email}")
        print(f"proposed_percent: {proposed_percent}")

        # check all fields are present
        if not all([property_id, from_owner_email, to_owner_email, proposed_percent]):
            response["message"] = (
                "Missing required fields: property_id, from_owner_email, to_owner_email, proposed_percent"
            )
            return response, 400

        # Validate proposed_percent is a number
        try:
            proposed_percent = float(proposed_percent)
            if proposed_percent <= 0 or proposed_percent > 100:
                response["message"] = "proposed_percent must be between 0 and 100"
                return response, 400
        except (ValueError, TypeError):
            response["message"] = "proposed_percent must be a valid number"
            return response, 400

        with connect() as db:
            # Prioritize owner profiles linked to user accounts (owner_user_id IS NOT NULL)
            fromOwnerQuery = db.execute(
                """
                SELECT owner_uid, owner_user_id
                FROM space_dev.ownerProfileInfo 
                WHERE owner_email = %s
                ORDER BY 
                    CASE WHEN owner_user_id IS NOT NULL THEN 0 ELSE 1 END,
                    owner_uid DESC
                """,
                (from_owner_email,),
            )

            if not fromOwnerQuery["result"]:
                response["message"] = (
                    f"Owner with email {from_owner_email} does not exist"
                )
                return response, 404

            from_owner_id = fromOwnerQuery["result"][0]["owner_uid"]
            print(
                f"from_owner_id found: {from_owner_id} (from {len(fromOwnerQuery['result'])} possible owner profile(s))"
            )

            # to_owner_id from email to uid
            # Prioritize owner profiles linked to user accounts (owner_user_id IS NOT NULL)
            toOwnerQuery = db.execute(
                """
                SELECT owner_uid, owner_user_id
                FROM space_dev.ownerProfileInfo 
                WHERE owner_email = %s
                ORDER BY 
                    CASE WHEN owner_user_id IS NOT NULL THEN 0 ELSE 1 END,
                    owner_uid DESC
                """,
                (to_owner_email,),
            )

            print(f" toOwnerQuery result: {toOwnerQuery}")

            if not toOwnerQuery["result"]:
                response["message"] = (
                    f"Owner with email {to_owner_email} does not exist"
                )
                return response, 404

            to_owner_id = toOwnerQuery["result"][0]["owner_uid"]
            print(
                f"to_owner_id found: {to_owner_id} (from {len(toOwnerQuery['result'])} possible owner profile(s))"
            )

            checkOwnerQuery = db.execute(
                """
                SELECT po_owner_percent 
                FROM space_dev.property_owner 
                WHERE property_id = %s AND property_owner_id = %s
                """,
                (property_id, from_owner_id),
            )

            if not checkOwnerQuery["result"]:
                response["message"] = (
                    f"Owner {from_owner_email} does not own property {property_id}"
                )
                return response, 404

            current_percent = (
                float(checkOwnerQuery["result"][0]["po_owner_percent"]) * 100
            )

            if current_percent < proposed_percent:
                response["message"] = (
                    f"Owner {from_owner_email} only has {current_percent}% ownership, cannot transfer {proposed_percent}%"
                )
                return response, 400

            # Generate next transfer ID
            maxIdQuery = db.execute(
                """
                SELECT MAX(transfer_id) as max_id 
                FROM space_dev.ownershipTransfer
                """
            )

            if maxIdQuery["result"] and maxIdQuery["result"][0]["max_id"]:
                max_id = maxIdQuery["result"][0]["max_id"]

                numeric_part = int(max_id.split("-")[1]) + 1
                new_transfer_id = f"350-{numeric_part:06d}"
            else:
                new_transfer_id = "350-000001"

            print(" About to INSERT ")
            print(f"new_transfer_id: {new_transfer_id}")
            print(f"property_id: {property_id}")
            print(f"from_owner_id (ownerId): {from_owner_id}")
            print(f"to_owner_id: {to_owner_id}")
            print(f"current_percent: {current_percent}")
            print(f"proposed_percent: {proposed_percent}")

            insertQuery = db.execute(
                """
                INSERT INTO space_dev.ownershipTransfer 
                (transfer_id, property_id, ownerId, to_owner_id, current_percent, proposed_percent, transfer_status)
                VALUES 
                (%s, %s, %s, %s, %s, %s, 'PENDING')
                """,
                (
                    new_transfer_id,
                    property_id,
                    from_owner_id,
                    to_owner_id,
                    current_percent,
                    proposed_percent,
                ),
                "post",
            )

            print(f"INSERT Result: {insertQuery}")
            print(f"INSERT affected rows: {insertQuery.get('affected_rows', 'N/A')}")

            print(f"\n POST: About to send announcement...")
            print(f"   from_owner_id (sender): {from_owner_id}")
            print(f"   to_owner_id (receiver): {to_owner_id}")
            print(f"   transfer_id: {new_transfer_id}")
            print(f"   property_id: {property_id}")
            print(f"   proposed_percent: {proposed_percent}")

            # Send announcement to the to_owner about the new transfer request
            announcement_result = send_ownership_announcement(
                db=db,
                sender_id=from_owner_id,
                receiver_id=to_owner_id,
                transfer_id=new_transfer_id,
                property_id=property_id,
                percent=proposed_percent,
                mode="REQUEST",
            )

            print(f" POST: Announcement result: {announcement_result}")

            # Get the created transfer
            getTransferQuery = db.execute(
                """
                SELECT * FROM space_dev.ownershipTransfer 
                WHERE property_id = %s AND ownerId = %s AND to_owner_id = %s 
                ORDER BY transfer_id DESC LIMIT 1
                """,
                (property_id, from_owner_id, to_owner_id),
            )

            print(f"=== Created Transfer Record: {getTransferQuery['result']}")

            response["message"] = "Transfer request created successfully"
            response["transfer"] = (
                getTransferQuery["result"][0] if getTransferQuery["result"] else {}
            )

            print(f"=== Returning response: {response}")

        return response, 200

    def put(self):
        print("In OwnershipTransfer PUT")
        response = {}

        print("=== DEBUG: request.form type:", type(request.form))
        print("=== DEBUG: request.form keys:", list(request.form.keys()))
        print("=== DEBUG: request.form dict:", dict(request.form))
        print("=== DEBUG: request.content_type:", request.content_type)

        payload = request.form
        print("OwnershipTransfer PUT Payload: ", payload)

        transfer_id = payload.get("transfer_id")
        action = payload.get("action")
        user_id = payload.get("user_id")

        if not all([transfer_id, action, user_id]):
            response["message"] = (
                "Missing required fields: transfer_id, action, user_id"
            )
            return response, 400

        action = action.upper()
        if action not in ["ACCEPT", "DECLINE", "CANCEL"]:
            response["message"] = "action must be ACCEPT, DECLINE, or CANCEL"
            return response, 400

        with connect() as db:

            transferQuery = db.execute(
                """
                SELECT * FROM space_dev.ownershipTransfer 
                WHERE transfer_id = %s
                """,
                (transfer_id,),
            )

            if not transferQuery["result"]:
                response["message"] = f"Transfer {transfer_id} not found"
                return response, 404

            transfer = transferQuery["result"][0]

            if transfer["transfer_status"] != "PENDING":
                response["message"] = (
                    f"Transfer is already {transfer['transfer_status']}, cannot modify"
                )
                return response, 400

            # Handle CANCEL action: only from owner can cancel
            if action == "CANCEL":
                if user_id != transfer["ownerId"]:
                    response["message"] = "Only the from_owner can cancel this transfer"
                    return response, 403

                db.execute(
                    """
                    UPDATE space_dev.ownershipTransfer 
                    SET transfer_status = 'CANCELLED' 
                    WHERE transfer_id = %s
                    """,
                    (transfer_id,),
                )

                response["message"] = "Transfer request cancelled successfully"
                return response, 200

            # Handle DECLINE action: ony to_owner can decline
            elif action == "DECLINE":
                if user_id != transfer["to_owner_id"]:
                    response["message"] = "Only the to_owner can decline this transfer"
                    return response, 403

                property_id = transfer["property_id"]
                from_owner_id = transfer["ownerId"]
                to_owner_id = transfer["to_owner_id"]
                proposed_percent = float(transfer["proposed_percent"])

                db.execute(
                    """
                    UPDATE space_dev.ownershipTransfer 
                    SET transfer_status = 'REJECTED' 
                    WHERE transfer_id = %s
                    """,
                    (transfer_id,),
                )

                print(f"\n DECLINE: About to send announcement...")
                print(f"   to_owner_id (sender): {to_owner_id}")
                print(f"   from_owner_id (receiver): {from_owner_id}")
                print(f"   transfer_id: {transfer_id}")
                print(f"   property_id: {property_id}")
                print(f"   proposed_percent: {proposed_percent}")

                announcement_result = send_ownership_announcement(
                    db,
                    to_owner_id,
                    from_owner_id,
                    transfer_id,
                    property_id,
                    proposed_percent,
                    "REJECTED",
                )

                print(f"DECLINE: Announcement result: {announcement_result}")

                response["message"] = "Transfer request declined successfully"
                return response, 200

            # Action :accept :only to_owner can accept

            elif action == "ACCEPT":

                if user_id != transfer["to_owner_id"]:
                    response["message"] = "Only the to_owner can accept this transfer"
                    return response, 403

                property_id = transfer["property_id"]
                from_owner_id = transfer["ownerId"]
                to_owner_id = transfer["to_owner_id"]
                proposed_percent = float(transfer["proposed_percent"])
                current_percent = float(transfer["current_percent"])

                # Calculate remaining percent for from_owner after deduction
                remaining_percent = current_percent - proposed_percent

                print(f"\nACCEPT: Processing transfer approval.")
                print(f" from_owner_id: {from_owner_id}")
                print(f" to_owner_id: {to_owner_id}")
                print(f" property_id: {property_id}")
                print(f" current_percent: {current_percent}%")
                print(f" proposed_percent: {proposed_percent}%")
                print(f" remaining_percent: {remaining_percent}%")

                # 1. Update from_owner's percent in property_owner (the main ownership table)
                update_from_result = db.execute(
                    """
                    UPDATE space_dev.property_owner 
                    SET po_owner_percent = %s 
                    WHERE property_id = %s AND property_owner_id = %s
                    """,
                    (remaining_percent / 100, property_id, from_owner_id),
                )
                print(f"   Update from_owner result: {update_from_result}")

                # 2. Check if to_owner already has ownership in this property
                checkToOwnerQuery = db.execute(
                    """
                    SELECT po_owner_percent 
                    FROM space_dev.property_owner 
                    WHERE property_id = %s AND property_owner_id = %s
                    """,
                    (property_id, to_owner_id),
                )
                print(f"   Check to_owner result: {checkToOwnerQuery}")

                if checkToOwnerQuery["result"]:
                    # to_owner already owns part of this property, update their percent
                    existing_to_percent = (
                        float(checkToOwnerQuery["result"][0]["po_owner_percent"]) * 100
                    )
                    new_to_percent = existing_to_percent + proposed_percent
                    update_to_result = db.execute(
                        """
                        UPDATE space_dev.property_owner 
                        SET po_owner_percent = %s 
                        WHERE property_id = %s AND property_owner_id = %s
                        """,
                        (new_to_percent / 100, property_id, to_owner_id),
                    )
                    print(f"   Update to_owner result: {update_to_result}")
                else:
                    # to_owner is new to this property, insert them
                    insert_to_result = db.execute(
                        """
                        INSERT INTO space_dev.property_owner 
                        (property_id, property_owner_id, po_owner_percent) 
                        VALUES (%s, %s, %s)
                        """,
                        (property_id, to_owner_id, proposed_percent / 100),
                    )
                    print(f"   Insert to_owner result: {insert_to_result}")

                # 3. Leave the original PENDING record as-is (keeps history of the request)
                #    Create a NEW APPROVED record with current_percent = remaining after deduction
                #    e.g. original: current=90, proposed=20, PENDING
                #         new:     current=70, proposed=20, APPROVED
                maxIdQuery = db.execute(
                    """
                    SELECT MAX(transfer_id) as max_id 
                    FROM space_dev.ownershipTransfer
                    """
                )

                if maxIdQuery["result"] and maxIdQuery["result"][0]["max_id"]:
                    max_id = maxIdQuery["result"][0]["max_id"]
                    numeric_part = int(max_id.split("-")[1]) + 1
                    new_transfer_id = f"350-{numeric_part:06d}"
                else:
                    new_transfer_id = "350-000001"

                db.execute(
                    """
                    INSERT INTO space_dev.ownershipTransfer 
                    (transfer_id, property_id, ownerId, to_owner_id, current_percent, proposed_percent, transfer_status)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, 'APPROVED')
                    """,
                    (
                        new_transfer_id,
                        property_id,
                        from_owner_id,
                        to_owner_id,
                        remaining_percent,
                        proposed_percent,
                    ),
                    "post",
                )
                print(f"   Original PENDING transfer {transfer_id} kept as-is")
                print(
                    f"   New APPROVED transfer {new_transfer_id} created: current_percent={remaining_percent}, proposed_percent={proposed_percent}"
                )

                # 4. Send notification to from_owner about approval
                announcement_result = send_ownership_announcement(
                    db,
                    to_owner_id,
                    from_owner_id,
                    new_transfer_id,
                    property_id,
                    proposed_percent,
                    "APPROVED",
                )

                print(f"ACCEPT: Announcement result: {announcement_result}")

                response["message"] = (
                    "Transfer accepted and ownership updated successfully"
                )
                response["pending_transfer_id"] = transfer_id
                response["approved_transfer_id"] = new_transfer_id
                response["from_owner_new_percent"] = remaining_percent
                response["to_owner_new_percent"] = proposed_percent

        return response, 200


class OwnershipTransfersByUser(Resource):
    """Get all pending ownership transfers for a specific user"""

    def get(self, user_id):
        print(f"\n In OwnershipTransfersByUser GET for user: {user_id}")
        response = {}

        if not user_id:
            response["message"] = "Please provide user_id"
            return response

        with connect() as db:
            print(f" Fetching pending transfers where to_owner_id = {user_id}")

            ownershipQuery = db.execute(
                """
                SELECT 
                    ot.transfer_id,
                    ot.property_id,
                    ot.ownerId,
                    ot.to_owner_id,
                    ot.current_percent,
                    ot.proposed_percent,
                    ot.transfer_status,
                    opi.owner_email,
                    opi.owner_first_name,
                    opi.owner_last_name,
                    to_owner.owner_email AS to_owner_email,
                    to_owner.owner_first_name AS to_owner_first_name,
                    to_owner.owner_last_name AS to_owner_last_name,
                    po.po_owner_percent,
                    po.po_start_date,
                    po.po_end_date,
                    prop.property_address,
                    prop.property_unit,
                    prop.property_city,
                    prop.property_state,
                    prop.property_zip
                FROM space_dev.ownershipTransfer AS ot
                LEFT JOIN space_dev.ownerProfileInfo AS opi ON ot.ownerId = opi.owner_uid
                LEFT JOIN space_dev.ownerProfileInfo AS to_owner ON ot.to_owner_id = to_owner.owner_uid
                LEFT JOIN space_dev.property_owner AS po ON ot.property_id = po.property_id AND ot.ownerId = po.property_owner_id
                LEFT JOIN space_dev.properties AS prop ON ot.property_id = prop.property_uid
                WHERE ot.to_owner_id = %s AND ot.transfer_status = 'PENDING'
                AND NOT EXISTS (
                    SELECT 1 FROM space_dev.ownershipTransfer AS approved
                    WHERE approved.property_id = ot.property_id
                    AND approved.ownerId = ot.ownerId
                    AND approved.to_owner_id = ot.to_owner_id
                    AND approved.proposed_percent = ot.proposed_percent
                    AND approved.transfer_status = 'APPROVED'
                    AND approved.transfer_id > ot.transfer_id
                )
                ORDER BY ot.transfer_id DESC
                """,
                (user_id,),
            )

            response["result"] = ownershipQuery.get("result", [])
            response["count"] = len(response["result"])
            response["message"] = (
                f"Found {response['count']} pending ownership transfer(s) for user {user_id}"
            )

            print(f" Found {response['count']} pending transfer(s)")
            if response["count"] > 0:
                print(f" Transfers: {[t['transfer_id'] for t in response['result']]}")

        return response

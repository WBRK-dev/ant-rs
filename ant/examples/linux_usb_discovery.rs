use ant::drivers::*;
use ant::messages::config::{AssignChannel, ChannelId, ChannelPeriod, ChannelRfFrequency, ChannelType, DeviceType, LibConfig, SetNetworkKey, TransmissionType, UnAssignChannel};
use ant::messages::control::{OpenChannel, OpenRxScanMode, ResetSystem};
use ant::messages::data::{AcknowledgedData, BroadcastData};
use rusb::{Device, DeviceList};

use dialoguer::Select;

fn main() {
    let mut devices: Vec<Device<_>> = DeviceList::new()
        .expect("Unable to lookup usb devices")
        .iter()
        .filter(|x| is_ant_usb_device_from_device(x))
        .collect();

    if devices.is_empty() {
        panic!("No devices found");
    }

    let device = if devices.len() == 1 {
        devices.remove(0)
    } else {
        let selection = Select::new()
            .with_prompt("Multiple devices found, please select a radio to use.")
            .items(
                &devices
                    .iter()
                    .map(|x| x.device_descriptor().unwrap())
                    .map(|x| format!("{:04x}:{:04x}", x.vendor_id(), x.product_id()))
                    .collect::<Vec<String>>(),
            )
            .interact()
            .expect("Dialogue error");
        devices.remove(selection)
    };

    let mut driver = UsbDriver::new(device).unwrap();
    let assign = AssignChannel::new(0, ChannelType::BidirectionalSlave, 0, None);
    let key = SetNetworkKey::new(0, [0xB9, 0xA5, 0x21, 0xFB, 0xBD, 0x72, 0xC3, 0x45]);
    // let key = SetNetworkKey::new(0, [0, 0, 0, 0, 0, 0, 0, 0]);
    let rf = ChannelRfFrequency::new(0, 57);
    let id = ChannelId::new(
        0,
        0,
        // DeviceType::new(17.into(), true),
        DeviceType::new_wildcard(),
        TransmissionType::new_wildcard(),
    );
    let period = ChannelPeriod::new(0, 8192);
    let libconfig = LibConfig::new(true, true, true);
    driver
        .send_message(&ResetSystem::new())
        .expect("Failed to reset device");
    driver.send_message(&key).expect("Message failed");
    driver.send_message(&assign).expect("Message failed");
    driver.send_message(&id).expect("Message failed");
    driver.send_message(&period).expect("Message failed");
    driver.send_message(&rf).expect("Message failed");
    driver.send_message(&libconfig).expect("Message failed");
    driver
        .send_message(&OpenRxScanMode::new(Some(false)))
        .expect("Message failed");

    loop {
        match driver.get_message() {
            Ok(None) => (),
            Ok(Some(msg)) => {
                match &msg.message {
                    ant::messages::RxMessage::ChannelEvent(x) => println!("{:#?}", x.payload.message_code),
                    ant::messages::RxMessage::BroadcastData(x) => {
                        println!(
                            "Type: {}, Device number: {}, pairing bit: {}",
                            x.extended_info.unwrap().channel_id_output.unwrap().device_type.device_type_id,
                            x.extended_info.unwrap().channel_id_output.unwrap().device_number,
                            x.extended_info.unwrap().channel_id_output.unwrap().device_type.pairing_request
                        );
                        if x.extended_info.unwrap().channel_id_output.unwrap().device_type.device_type_id != 17.into() {
                            continue;
                        }
                    }
                    _ => println!("{:#?}", msg),
                }
            },
            msg => println!("{:#?}", msg),
        }
    }
}

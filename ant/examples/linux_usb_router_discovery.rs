use std::thread::sleep;
use std::time::Duration;

use ant::channel::{RxError, RxHandler, TxError, TxHandler};
use ant::drivers::*;
use ant::messages::config::{AssignChannel, ChannelId, ChannelPeriod, ChannelRfFrequency, ChannelType, DeviceType, LibConfig, SetNetworkKey, TransmissionType, UnAssignChannel};
use ant::messages::control::{OpenChannel, OpenRxScanMode, ResetSystem};
use ant::messages::data::{AcknowledgedData, BroadcastData};
use ant::router::Router;
use rusb::{Device, DeviceList};

use dialoguer::Select;

use thingbuf::mpsc::errors::{TryRecvError, TrySendError};
use thingbuf::mpsc::{channel, Receiver, Sender};

struct TxSender<T> {
    sender: Sender<T>,
}

struct RxReceiver<T> {
    receiver: Receiver<T>,
}

impl<T: Default + Clone> TxHandler<T> for TxSender<T> {
    fn try_send(&self, msg: T) -> Result<(), TxError> {
        match self.sender.try_send(msg) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => Err(TxError::Full),
            Err(TrySendError::Closed(_)) => Err(TxError::Closed),
            Err(_) => Err(TxError::UnknownError),
        }
    }
}

impl<T: Default + Clone> RxHandler<T> for RxReceiver<T> {
    fn try_recv(&self) -> Result<T, RxError> {
        match self.receiver.try_recv() {
            Ok(e) => Ok(e),
            Err(TryRecvError::Empty) => Err(RxError::Empty),
            Err(TryRecvError::Closed) => Err(RxError::Closed),
            Err(_) => Err(RxError::UnknownError),
        }
    }
}

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

    let driver = UsbDriver::new(device).unwrap();

    let (channel_tx, router_rx) = channel(16);
    let (router_tx, channel_rx) = channel(16);

    let mut router = Router::new(
        driver,
        RxReceiver {
            receiver: router_rx,
        },
    )
    .unwrap();

    router.set_rx_message_callback(Some(|msg| {
        println!("{:?}", msg);
    }));

    let snk = SetNetworkKey::new(0, [0xB9, 0xA5, 0x21, 0xFB, 0xBD, 0x72, 0xC3, 0x45]);
    router.send(&snk).expect("failed to set network key");
    let chan = router
        .add_channel(TxSender { sender: router_tx })
        .expect("Add channel failed");

    let assign = AssignChannel::new(chan, ChannelType::BidirectionalSlave, 0, None);
    let rf = ChannelRfFrequency::new(chan, 57);
    let id = ChannelId::new(
        chan,
        0,
        DeviceType::new_wildcard(),
        TransmissionType::new_wildcard(),
    );
    let period = ChannelPeriod::new(chan, 8192);
    let libconfig = LibConfig::new(true, true, true);
    channel_tx.try_send(assign.into()).expect("Message failed");
    channel_tx.try_send(id.into()).expect("Message failed");
    channel_tx.try_send(period.into()).expect("Message failed");
    channel_tx.try_send(rf.into()).expect("Message failed");
    channel_tx.try_send(libconfig.into()).expect("Message failed");

    channel_tx.try_send(OpenRxScanMode::new(Some(false)).into())
        .expect("Message failed");

    let broadcast_data: BroadcastData;
    loop {
        router.process();
        match channel_rx.try_recv() {
            Ok(msg) => {
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

                        // broadcast_data = *x;
                        // break;
                    }
                    _ => println!("{:#?}", msg),
                }
            },
            Err(TryRecvError::Empty) => {},
            Err(err) => println!("{:?}", err),
            // msg => println!("{:#?}", msg),
            _ => {},
        }
        sleep(Duration::from_millis(100));
    }

    let chan = 0;
    driver.send_message(&UnAssignChannel::new(chan)).expect("Unable to unassign channel");
    driver.send_message(&ResetSystem::new()).expect("Unable to reset system");

    // wait 5 seconds
    std::thread::sleep(std::time::Duration::from_secs(5));

    // driver.send_message(&key).expect("Message failed");

    let assign = AssignChannel::new(chan, ChannelType::BidirectionalSlave, 0, None);
    let id = ChannelId::new(
        chan,
        broadcast_data.extended_info.unwrap().channel_id_output.unwrap().device_number,
        DeviceType::new(17.into(), true),
        TransmissionType::new_wildcard()
    );
    let rf = ChannelRfFrequency::new(chan, 57);
    let period = ChannelPeriod::new(chan, 8192);

    // driver
    //     .send_message(&ResetSystem::new())
    //     .expect("Failed to reset device");

    // driver.send_message(&key).expect("Message failed");
    driver.send_message(&assign).expect("Message failed");
    println!("Sending id");
    driver.send_message(&id).expect("Message failed");
    driver.send_message(&period).expect("Message failed");
    driver.send_message(&rf).expect("Message failed");
    driver.send_message(&libconfig).expect("Message failed");
    driver.send_message(&OpenChannel::new(0)).expect("Unable to open channel");
    // driver
    //     .send_message(&OpenRxScanMode::new(Some(false)))
    //     .expect("Message failed");

    let mut has_send_target_power = false;
    let mut last_send_time = std::time::Instant::now();

    loop {
        match driver.get_message() {
            Ok(None) => (),
            msg => {
                match msg {
                    Ok(Some(msg)) => {
                        if let ant::messages::RxMessage::ChannelEvent(x) = &msg.message {
                            println!("Channel event: {:?}", x);
                            continue;
                        }

                        // if !has_send_target_power {
                        //     println!("Received message: (hidden)");
                        // } else {
                        // }
                        //     println!("{:#?}", msg.message);

                        if !has_send_target_power && last_send_time.elapsed().as_secs() > 5 {
                            let target_power = 100;
                            println!("Setting target power to {} : {}", (target_power & 0xFF) as u8, (target_power >> 8) as u8);
                            let message: ant::messages::TxMessage = AcknowledgedData::new(chan, [
                                0x31,
                                0xFF,
                                0xFF,
                                0xFF,
                                0xFF,
                                0xFF,
                                (target_power & 0xFF) as u8,
                                (target_power >> 8) as u8,
                            ]).into();
                            // let message: ant::messages::TxMessage = AcknowledgedData::new(chan, [
                            //     0x46,
                            //     0xFF,
                            //     0xFF,
                            //     0x00,
                            //     0x00,
                            //     0x80,
                            //     0x31,
                            //     0x01,
                            // ]).into();
                            driver.send_message(&message).expect("Failed to send message");
                            println!("Sent target power");
                            has_send_target_power = true;
                            last_send_time = std::time::Instant::now();
                        }
                    },
                    Ok(None) => println!("No message received"),
                    Err(e) => println!("Error receiving message: {:?}", e)
                }
            },
        }
    }
}

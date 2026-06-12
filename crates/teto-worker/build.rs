use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=RIVA_PROTO_DIR");
    println!("cargo:rerun-if-env-changed=RIVA_PROTO_FILES");
    println!("cargo:rerun-if-env-changed=RIVA_PROTO_INCLUDE");

    let proto_files = riva_proto_files();
    if proto_files.is_empty() {
        let message = "RIVA_PROTO_DIR/RIVA_PROTO_FILES is required with --features riva; \
                       point it at nvidia-riva/common/riva/proto or set RIVA_PROTO_FILES explicitly";
        println!("cargo:warning={message}");
        if std::env::var_os("CARGO_FEATURE_RIVA").is_some() {
            panic!("{message}");
        }
        return;
    }

    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let include_dir = riva_proto_include_dir(&proto_files);
    println!("cargo:rerun-if-changed={}", include_dir.display());

    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR is set by Cargo"));
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .file_descriptor_set_path(out_dir.join("riva_descriptor.bin"))
        .compile_protos(&proto_files, &[include_dir])
        .expect("failed to compile NVIDIA Riva protobuf definitions");
}

fn riva_proto_files() -> Vec<PathBuf> {
    if let Ok(files) = std::env::var("RIVA_PROTO_FILES") {
        let mut proto_files = Vec::new();
        for file in files
            .split(',')
            .map(str::trim)
            .filter(|file| !file.is_empty())
        {
            proto_files.push(PathBuf::from(file));
        }
        return proto_files;
    }

    let proto_dir = match std::env::var("RIVA_PROTO_DIR") {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => return Vec::new(),
    };

    let candidates = ["riva_asr.proto", "riva_audio.proto", "riva_common.proto"];
    let proto_files = candidates
        .into_iter()
        .map(|file| proto_dir.join(file))
        .filter(|file| file.exists())
        .collect::<Vec<_>>();

    if proto_files.is_empty() {
        panic!(
            "RIVA_PROTO_DIR points to {}, but no Riva proto files were found. \
             Point it at nvidia-riva/common/riva/proto or set RIVA_PROTO_FILES explicitly.",
            proto_dir.display()
        );
    }

    proto_files
}

fn riva_proto_include_dir(proto_files: &[PathBuf]) -> PathBuf {
    std::env::var_os("RIVA_PROTO_INCLUDE")
        .map(PathBuf::from)
        .or_else(|| {
            proto_files
                .first()
                .and_then(|file| infer_riva_include_dir(file))
        })
        .unwrap_or_else(|| PathBuf::from("."))
}

fn infer_riva_include_dir(proto_file: &Path) -> Option<PathBuf> {
    let parent = proto_file.parent()?;
    if parent.file_name()?.to_string_lossy() == "proto" {
        let riva_dir = parent.parent()?;
        if riva_dir.file_name()?.to_string_lossy() == "riva" {
            return riva_dir.parent().map(Path::to_path_buf);
        }
    }

    Some(parent.to_path_buf())
}

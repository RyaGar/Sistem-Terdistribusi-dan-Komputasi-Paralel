import random


class QuorumNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.storage = {}
        self.version = {}
        self.is_alive = True

    def write_replica(self, key, val, ver):
        if not self.is_alive:
            return False
        self.storage[key] = val
        self.version[key] = ver
        return True

    def read_replica(self, key):
        if not self.is_alive:
            return None, -1
        return self.storage.get(key, None), self.version.get(key, 0)


class QuorumCluster:
    def __init__(self, total_nodes=5, n_read=3, n_write=3):
        self.total_nodes = total_nodes
        self.n_read = n_read
        self.n_write = n_write
        self.nodes = [QuorumNode(i) for i in range(total_nodes)]
        # Semua node awalnya berada di partisi yang sama ("ALL")
        self.partition_of = {i: "ALL" for i in range(total_nodes)}
        self._validate_quorum_rules()

    def _validate_quorum_rules(self):
        rule1 = (self.n_read + self.n_write) > self.total_nodes
        rule2 = self.n_write > (self.total_nodes / 2)
        print("=== CLUSTER QUORUM INITIALIZATION ===")
        print(f"Total Nodes (N): {self.total_nodes} | N_R: {self.n_read} | N_W: {self.n_write}")
        print(f"Rule 1 (N_R + N_W > N) -> {rule1} (Read-Write Overlap Terjamin)")
        print(f"Rule 2 (N_W > N/2)     -> {rule2} (Write-Write Overlap / Anti-Split-Brain Terjamin)")
        if not (rule1 and rule2):
            raise ValueError("Konfigurasi Quorum melanggar aturan Gifford!")

    # ---------- Network Partition ----------
    def create_partition(self, groups):
        """groups: dict {nama_partisi: [node_id, ...]}"""
        for name, ids in groups.items():
            for i in ids:
                self.partition_of[i] = name
        print("\n>>> NETWORK PARTITION terjadi: " +
              " | ".join(f"Partisi {n}: Node-{ids}" for n, ids in groups.items()) + " <<<")

    def heal_partition(self):
        for i in self.partition_of:
            self.partition_of[i] = "ALL"
        print("\n>>> Partisi jaringan PULIH (semua node terhubung kembali) <<<")

    def _reachable_nodes(self, group=None):
        """Node yang hidup dan dapat dijangkau client pada partisi 'group'."""
        return [n for n in self.nodes
                if n.is_alive and (group is None or self.partition_of[n.node_id] == group)]

    def _pick_quorum(self, reachable, size, forced_ids=None):
        if forced_ids is not None:
            reachable_ids = {n.node_id for n in reachable}
            if not set(forced_ids).issubset(reachable_ids):
                print(f"[ERROR] Kuorum paksa {forced_ids} tidak seluruhnya terjangkau!")
                return None
            return [self.nodes[i] for i in forced_ids]
        return random.sample(reachable, size)

    # ---------- Write ----------
    def client_write(self, key, value, group=None, quorum_ids=None):
        loc = f" (dari Partisi {group})" if group else ""
        print(f"\n[CLIENT WRITE]{loc} Inisiasi Write: {key} = '{value}'")
        reachable = self._reachable_nodes(group)

        if len(reachable) < self.n_write:
            print(f"[WRITE REJECTED] Node terjangkau ({len(reachable)}) kurang dari N_W ({self.n_write})!")
            return False

        write_quorum = self._pick_quorum(reachable, self.n_write, quorum_ids)
        if write_quorum is None:
            return False

        max_ver = 0
        for node in write_quorum:
            _, ver = node.read_replica(key)
            if ver > max_ver:
                max_ver = ver
        new_ver = max_ver + 1

        written_ids = [node.node_id for node in write_quorum
                       if node.write_replica(key, value, new_ver)]
        print(f"[WRITE SUCCESS] Ditulis ke {len(written_ids)} node kuorum: {written_ids} (Version: {new_ver})")
        return True

    # ---------- Read + Read-Repair ----------
    def client_read(self, key, group=None, quorum_ids=None):
        loc = f" (dari Partisi {group})" if group else ""
        print(f"\n[CLIENT READ]{loc} Inisiasi Read untuk key: '{key}'")
        reachable = self._reachable_nodes(group)

        if len(reachable) < self.n_read:
            print(f"[READ REJECTED] Node terjangkau ({len(reachable)}) kurang dari N_R ({self.n_read})!")
            return None

        read_quorum = self._pick_quorum(reachable, self.n_read, quorum_ids)
        if read_quorum is None:
            return None

        latest_val = None
        latest_ver = -1
        sampled_data = []
        for node in read_quorum:
            val, ver = node.read_replica(key)
            sampled_data.append((node.node_id, val, ver))
            if ver > latest_ver:
                latest_ver = ver
                latest_val = val

        quorum_ids_used = [node.node_id for node in read_quorum]
        print(f"[READ SAMPLES] Node Kuorum {quorum_ids_used}: {sampled_data}")
        print(f"[READ RESULT] Nilai Terkini: '{latest_val}' (Version Tertinggi: {latest_ver})")

        # --- Read-Repair (Anti-Entropy) ---
        if latest_ver > 0:
            for node_id, _, ver in sampled_data:
                if ver < latest_ver:
                    print(f"[READ-REPAIR] Node-{node_id} tertinggal (v{ver} < v{latest_ver}). "
                          f"Mengirim pembaruan...")
                    self.nodes[node_id].write_replica(key, latest_val, latest_ver)
                    print(f"[READ-REPAIR] Node-{node_id} berhasil diperbarui -> "
                          f"'{latest_val}' (Version: {latest_ver})")
        return latest_val

    def dump_state(self, key):
        print(f"\n[STATE] Isi key '{key}' di seluruh node:")
        for n in self.nodes:
            status = "UP  " if n.is_alive else "DOWN"
            print(f"   Node-{n.node_id} [{status}] [Part {self.partition_of[n.node_id]}] -> "
                  f"val={n.storage.get(key)}, ver={n.version.get(key, 0)}")


def run_quorum_demo():
    """Demo Praktik 2 (asli)."""
    cluster = QuorumCluster(total_nodes=5, n_read=3, n_write=3)

    cluster.client_write("saldo_rek", 5000000)
    cluster.client_read("saldo_rek")

    print("\n>>> Skenario: Node-0 dan Node-1 down (Sisa 3 node aktif) <<<")
    cluster.nodes[0].is_alive = False
    cluster.nodes[1].is_alive = False

    cluster.client_write("saldo_rek", 7500000)
    cluster.client_read("saldo_rek")

    print("\n>>> Skenario: Node-2 down (Sisa 2 node aktif) <<<")
    cluster.nodes[2].is_alive = False

    cluster.client_write("saldo_rek", 9000000)
    cluster.client_read("saldo_rek")


def run_partition_demo():
    """Tugas 1: Network Partition (Split-Brain Prevention)."""
    print("\n" + "=" * 60)
    print("TUGAS 1: NETWORK PARTITION")
    print("=" * 60)
    cluster = QuorumCluster(5, 3, 3)
    cluster.client_write("saldo_rek", 5000000)

    cluster.create_partition({"A": [0, 1, 2], "B": [3, 4]})

    cluster.client_write("saldo_rek", 6000000, group="A")  # harus SUKSES
    cluster.client_write("saldo_rek", 8000000, group="B")  # harus DITOLAK
    cluster.client_read("saldo_rek", group="B")            # ditolak (2 < N_R)
    cluster.dump_state("saldo_rek")

    cluster.heal_partition()
    # Setelah pulih, read dari kuorum {2,3,4}: Node-3/4 tertinggal -> read-repair
    cluster.client_read("saldo_rek", quorum_ids=[2, 3, 4])
    cluster.dump_state("saldo_rek")


def run_read_repair_demo():
    """Tugas 2: Read-Repair Mechanism."""
    print("\n" + "=" * 60)
    print("TUGAS 2: READ-REPAIR")
    print("=" * 60)
    cluster = QuorumCluster(5, 3, 3)

    # Write 1 ke kuorum {0,1,2}, Write 2 ke kuorum {2,3,4} -> Node-0 & 1 tertinggal
    cluster.client_write("saldo_rek", 5000000, quorum_ids=[0, 1, 2])
    cluster.client_write("saldo_rek", 7500000, quorum_ids=[2, 3, 4])
    cluster.dump_state("saldo_rek")

    # Read dari kuorum {0,1,2}: Node-2 punya data terbaru, Node-0 & 1 diperbaiki
    cluster.client_read("saldo_rek", quorum_ids=[0, 1, 2])
    cluster.dump_state("saldo_rek")


if __name__ == '__main__':
    run_quorum_demo()
    run_partition_demo()
    run_read_repair_demo()
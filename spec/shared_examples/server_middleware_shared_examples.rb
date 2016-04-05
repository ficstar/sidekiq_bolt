shared_examples_for 'a server middleware' do
  it 'should yield' do
    expect { |block| subject.call(worker, job, nil, &block) }.to yield_control
  end

  it 'should return the result of the yielded block' do
    value = SecureRandom.uuid
    expect(subject.call(worker, job, nil) { value }).to eq(value)
  end
end
